use anyhow::{Error, Result};
use cli_common::{ColumnResult, ExprResult, ResultSet, StatementResult};
use deku::DekuReader;
use derive_more::derive::From;
use parser::ast::{Expr, Identifier, SelectExpressionBody, SelectItem, Statement, Value};
use thiserror::Error;

use crate::{
    catalog::DbLong,
    catalog::{Column, ColumnType, Database, Index, IndexType, Table, MASTER_DB_ID},
    engine::Storage,
    file_format::{FileType, SchemaInfo, SCHEMA_INFO_PAGE_INDEX},
    index_pager::IndexPager,
    page::PageDecoder,
    page_cache::FilePageId,
    persistence,
};

#[derive(Default)]
pub struct VirtualMachine;

#[derive(Debug, From, Error)]
enum StatementError {
    #[error("Database does not exist.")]
    DbDoesNotExist,
    #[error("Table does not exist.")]
    TableDoesNotExist,
    #[error("Column does not exist.")]
    ColumnsDoNotExist(Vec<SelectItem>),
}

#[derive(Debug, From, Error)]
enum SelectStatementError {
    #[error("Non-constant query contains no 'FROM' expression.")]
    NonConstantExprNoFrom,
}

#[derive(Debug)]
struct ExprResultWithPosition {
    pub pos: u8,
    pub expr: ExprResult,
}

#[derive(Debug)]
struct ColumnResultWithMetadata {
    pub pos: u8,
    pub col: ColumnResult,
    pub const_value: Option<ExprResult>,
}

impl VirtualMachine {
    pub fn execute_statement(
        &self,
        statement: &Statement,
        mut storage: &mut Storage,
    ) -> Result<StatementResult> {
        let is_const_expr = self.is_constant_statement(statement);

        if is_const_expr {
            log::debug!("Statement is constant");
            return self.evaluate_constant_statement(statement);
        }

        match statement {
            Statement::Select(s) => self.evaluate_select_statement(s, &mut storage),
            Statement::Update => todo!(),
            Statement::Insert => todo!(),
            Statement::Delete => todo!(),
            Statement::CreateTable(_) => todo!(),
            Statement::CreateDatabase(_) => todo!(),
        }
    }

    // todo: type?
    fn is_constant_statement(&self, statement: &Statement) -> bool {
        match statement {
            Statement::Select(select_expression_body) => select_expression_body
                .select_item_list
                .item_list
                .iter()
                .all(|item| self.is_const_exp(&item.expr)),
            Statement::Update => todo!(),
            Statement::Insert => todo!(),
            Statement::Delete => todo!(),
            Statement::CreateTable(_) => todo!(),
            Statement::CreateDatabase(_) => todo!(),
        }
    }

    fn is_const_exp(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Between {
                expr,
                lower,
                higher,
            } => self.is_const_exp(expr) && self.is_const_exp(lower) && self.is_const_exp(higher),
            Expr::NotBetween {
                expr,
                lower,
                higher,
            } => self.is_const_exp(expr) && self.is_const_exp(lower) && self.is_const_exp(higher),
            Expr::BinaryOperator { left, right, .. } => {
                self.is_const_exp(left) && self.is_const_exp(right)
            }
            Expr::IsFalse(expr) => self.is_const_exp(expr),
            Expr::IsTrue(expr) => self.is_const_exp(expr),
            Expr::IsNull(expr) => self.is_const_exp(expr),
            Expr::IsNotNull(expr) => self.is_const_exp(expr),
            Expr::Like { expr, pattern } => self.is_const_exp(expr) && self.is_const_exp(pattern),
            Expr::NotLike { expr, pattern } => {
                self.is_const_exp(expr) && self.is_const_exp(pattern)
            }
            Expr::IsIn { expr, list } => {
                self.is_const_exp(expr) && list.iter().all(|e| self.is_const_exp(e))
            }
            Expr::IsNotIn { expr, list } => {
                self.is_const_exp(expr) && list.iter().all(|e| self.is_const_exp(e))
            }
            Expr::IsNotFalse(expr) => self.is_const_exp(expr),
            Expr::IsNotTrue(expr) => self.is_const_exp(expr),
            Expr::Value(_) => true,
            Expr::QualifiedIdentifier(_) => false,
            Expr::Identifier(_) => false,
            Expr::Wildcard => false,
        }
    }

    fn evaluate_constant_statement(&self, statement: &Statement) -> Result<StatementResult> {
        match statement {
            Statement::Select(select_expression_body) => {
                let (columns, values) = select_expression_body
                    .select_item_list
                    .item_list
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        let alias = match &item.alias {
                            // TODO: clone
                            Some(ident) => Some(ident.value.clone()),
                            None => None,
                        };

                        let column = ColumnResult {
                            name: self.evaluate_column_name(&item.alias, index),
                            alias,
                        };

                        let value = self.evaluate_constant_expr(&item.expr);

                        (column, value)
                    })
                    .collect();

                //let values = columns.into_iter().map(|c| self.evaluate_constant_expr(&c.));

                Ok(StatementResult {
                    // TODO
                    result_set: ResultSet {
                        columns,
                        rows: vec![values],
                    },
                })
            }
            Statement::Update => todo!(),
            Statement::Insert => todo!(),
            Statement::Delete => todo!(),
            Statement::CreateTable(_) => todo!(),
            Statement::CreateDatabase(_) => todo!(),
        }
    }

    // TODO: refactor this monster
    fn evaluate_select_statement(
        &self,
        statement: &SelectExpressionBody,
        mut storage: &mut Storage,
    ) -> Result<StatementResult> {
        log::debug!("SELECT:");
        match &statement.from_clause {
            Some(from) => {
                let is_select_wildcard = statement
                    .select_item_list
                    .item_list
                    .iter()
                    .any(|i| *i == SelectItem::new(Expr::Wildcard));

                log::debug!("   ITEMS: {:?}", statement.select_item_list.item_list);

                let table_name = &from.identifier.value;
                let is_qualified = from.qualifier.is_some();

                match is_qualified {
                    true => log::debug!(
                        "   FROM: {}.{}",
                        &from.qualifier.as_ref().unwrap(),
                        table_name
                    ),
                    false => log::debug!("   FROM: {}", table_name),
                }

                // TODO: this is me just falling back to the master database if the user doesn't specify a db. not a final solution...
                let database_name = if is_qualified {
                    from.qualifier.as_ref().unwrap().value.clone()
                } else {
                    String::from("master")
                };

                // Step 1.
                // Fetch the `master` database file, we need to read a lot of metadata from it
                // in order to know where to read the user-query data from (and if it's even valid).
                let master_data_file = storage
                    .file_manager
                    .get_from_id(MASTER_DB_ID, FileType::Primary);

                if master_data_file.is_none() {
                    return Err(Error::msg("Failed to read Master data file"));
                }

                // Step 2.
                // Read the `master` SCHEMA_INFO page. This is going to tell us
                // where in the `master` DB to find info on the databases, tables, columns
                // and indexes that exist.
                let schema_page_bytes =
                    persistence::read_page(master_data_file.unwrap(), SCHEMA_INFO_PAGE_INDEX)?;

                let schema_page = PageDecoder::from_bytes(&schema_page_bytes);
                let schema_info = schema_page.try_read::<SchemaInfo>(0)?;

                // Step 3.
                // Create an pager and read all the Database records that exist
                // in the schema.
                // Validate if the database exists.
                let databases_page_iter = IndexPager::new(
                    FilePageId::new(MASTER_DB_ID, schema_info.databases_root_page_id),
                    &mut storage,
                );

                let mut dbs = databases_page_iter.map(|item| {
                    let mut cursor = std::io::Cursor::new(item);
                    let mut reader = deku::reader::Reader::new(&mut cursor);

                    return Database::from_reader_with_ctx(&mut reader, ());
                });

                let target_db = dbs.find(|i| {
                    i.as_ref()
                        .is_ok_and(|f| String::from_utf8(f.name.clone()).unwrap() == database_name)
                });

                if target_db.is_none() {
                    return Err(StatementError::DbDoesNotExist.into());
                }

                log::debug!("Validated database {} exists.", &database_name);

                // Step 4.
                // Create a pager and read all the Table records that exist in the schema.
                // Validate if the target table exists.
                let tables_page_iter = IndexPager::new(
                    FilePageId::new(MASTER_DB_ID, schema_info.tables_root_page_id),
                    &mut storage,
                );

                let mut tables = tables_page_iter.map(|item| {
                    let mut cursor = std::io::Cursor::new(item);
                    let mut reader = deku::reader::Reader::new(&mut cursor);

                    // TODO: handle Result
                    return Table::from_reader_with_ctx(&mut reader, ());
                });

                // TODO: unwrap
                // TODO: dont like having to deref the table_name pointer
                // TODO: dont like having to build a String manually
                let target_table = tables.find(|i| {
                    i.as_ref()
                        .is_ok_and(|f| String::from_utf8(f.name.clone()).unwrap() == *table_name)
                });

                if target_table.is_none() {
                    return Err(StatementError::TableDoesNotExist.into());
                }

                // TODO: unwraps
                let target_table_id = target_table.unwrap().unwrap().id;

                log::debug!("Validated table {} exists.", &table_name);

                // Step 5.
                // Create a pager and read from the Schema the indexes that exist.
                // Then find the Primary Key index on the target table.
                // This is needed to know where to start reading the data from.
                let indexes_page_iter = IndexPager::new(
                    FilePageId::new(MASTER_DB_ID, schema_info.indexes_root_page_id),
                    &mut storage,
                );

                let mut indexes = indexes_page_iter.map(|item| {
                    let mut cursor = std::io::Cursor::new(item);
                    let mut reader = deku::reader::Reader::new(&mut cursor);

                    return Index::from_reader_with_ctx(&mut reader, ());
                });

                // TODO: unwraps
                let pk_index_for_target_table = indexes
                    .find(|i| {
                        i.as_ref().is_ok_and(|f| {
                            f.table_id == target_table_id && f.index_type == IndexType::PK
                        })
                    })
                    .unwrap()
                    .unwrap();

                let target_table_index_root_id: DbLong = pk_index_for_target_table.root_page_id;

                log::debug!("Found PK index for table {}.", &table_name);

                // Step 6.
                // Create a pager and read all Columns from the Schema that exist.
                // Validate that the requested columns exist.
                let columns_page_iter = IndexPager::new(
                    FilePageId::new(MASTER_DB_ID, schema_info.columns_root_page_id),
                    &mut storage,
                );

                let columns = columns_page_iter.map(|item| {
                    let mut cursor = std::io::Cursor::new(item);
                    let mut reader = deku::reader::Reader::new(&mut cursor);

                    // TODO: unwrap
                    Column::from_reader_with_ctx(&mut reader, ()).unwrap()
                });

                let mut columns_of_target_table: Vec<Column> =
                    columns.filter(|c| c.table_id == target_table_id).collect();

                columns_of_target_table.sort_by(|a, b| a.position.cmp(&b.position));

                // Find the columns we need to process in the query.
                //      If wildcard, will be all columns sorted by the position property from master.columns,
                //      else will be sorted in the order in which the columns appeared in the query.
                let selected_columns: Vec<ColumnResultWithMetadata> = if is_select_wildcard {
                    // TODO: This needs work. It just dumps all columns as our selected, but doesn't account for const columns.
                    // Test: SELECT *, 'Hello' from Tables
                    columns_of_target_table
                        .iter()
                        .map(|col| {
                            let name = String::from_utf8(col.name.clone()).unwrap();

                            ColumnResultWithMetadata {
                                col: ColumnResult { name, alias: None },
                                pos: col.position,
                                const_value: None,
                            }
                        })
                        .collect()
                } else {
                    statement
                        .select_item_list
                        .item_list
                        .iter()
                        .enumerate()
                        .map(|(index, col)| {
                            let name = match &col.expr {
                                Expr::IsTrue(expr) => todo!(),
                                Expr::IsNotTrue(expr) => todo!(),
                                Expr::IsFalse(expr) => todo!(),
                                Expr::IsNotFalse(expr) => todo!(),
                                Expr::IsNull(expr) => todo!(),
                                Expr::IsNotNull(expr) => todo!(),
                                Expr::IsIn { expr, list } => todo!(),
                                Expr::IsNotIn { expr, list } => todo!(),
                                Expr::Between {
                                    expr,
                                    lower,
                                    higher,
                                } => todo!(),
                                Expr::NotBetween {
                                    expr,
                                    lower,
                                    higher,
                                } => todo!(),
                                Expr::Like { expr, pattern } => todo!(),
                                Expr::NotLike { expr, pattern } => todo!(),
                                Expr::BinaryOperator { left, op, right } => todo!(),
                                Expr::Value(_) => {
                                    let mut name = String::from("Column ");
                                    name.push_str(&index.to_string());

                                    name
                                }
                                Expr::Identifier(identifier) => identifier.value.clone(),
                                Expr::QualifiedIdentifier(identifiers) => {
                                    identifiers.identifier.value.clone()
                                }
                                Expr::Wildcard => unreachable!(),
                            };

                            let alias = match &col.alias {
                                // TODO: clone
                                Some(ident) => Some(ident.value.clone()),
                                None => None,
                            };

                            let const_value = if self.is_const_exp(&col.expr) {
                                Some(self.evaluate_constant_expr(&col.expr))
                            } else {
                                None
                            };

                            // TODO: pos may not be needed. sorting already works, even if all set to 0. Investigate that.
                            ColumnResultWithMetadata {
                                col: ColumnResult { name, alias },
                                pos: index as u8,
                                const_value,
                            }
                        })
                        .collect()
                };

                log::trace!("[EVAL SELECT] Selected columns: {:?}", selected_columns);

                // Step 6.5.
                // Validate that the requested columns exist.
                if !is_select_wildcard {
                    let missing_columns: Vec<SelectItem> = statement
                        .select_item_list
                        .item_list
                        .iter()
                        .filter(|i| match &i.expr {
                            // TODO: Handle all types of SelectItem expressions.
                            // TODO: I hate that we've turned this iter into a vec and now back into an iter
                            Expr::QualifiedIdentifier(ident) => columns_of_target_table
                                .iter()
                                // TODO: clone is a bit shit
                                .all(|c| {
                                    String::from_utf8(c.name.clone()).unwrap()
                                        != ident.identifier.value
                                }),
                            Expr::Identifier(ident) => columns_of_target_table
                                .iter()
                                // TODO: clone is a bit shit
                                .all(|c| String::from_utf8(c.name.clone()).unwrap() != ident.value),
                            Expr::Value(_) => false,
                            _ => {
                                log::trace!(
                                    "[EVAL SELECT] Expression {:?} defaulted to 'missing'.",
                                    i.expr
                                );
                                true
                            }
                        })
                        .cloned()
                        .collect();

                    if !missing_columns.is_empty() {
                        log::trace!("[EVAL SELECT] Missing columns: {:?}", missing_columns);
                        return Err(StatementError::ColumnsDoNotExist(missing_columns).into());
                    }
                }

                // Step 7.
                // Because deku wont work to deserialise a type it knows nothing about,
                // we need to use our column schema info to decide how to read the incoming row bytes.
                let target_table_iter = IndexPager::new(
                    FilePageId::new(MASTER_DB_ID, target_table_index_root_id.try_into().unwrap()),
                    &mut storage,
                );

                let mut results_with_positions: Vec<Vec<ExprResultWithPosition>> =
                    target_table_iter
                        .map(|row| {
                            let mut col_cursor = 0;
                            let mut byte_cursor = 0;
                            let mut results = Vec::new();

                            // While there are still bytes left to read in the row Vec<u8>
                            while byte_cursor < row.len() {

                                // Before we process the row of data from the table, just check if there's a const expr in the output.
                                // This is kinda a hack, because we're formatting the output results while enumerating the underlying table
                                // it makes it hard to select columns in a different order, or select columns that are const.
                                // I have a feeling this will need to be refactored when I get to joins, but for now this will work.
                                // TODO: There's a bug here where the order can be incorrect when the query includes const columns.
                                if let Some(col_at_pos) = selected_columns.iter().find(|c| c.pos == col_cursor) {
                                    if let Some(const_val) = &col_at_pos.const_value {
                                        results.push(ExprResultWithPosition { pos: 0, expr: const_val.clone() }); // TODO: pos needed? TODO: clone
                                    }
                                }

                                // Read the next column at the specified index (col_cursor)
                                let current_col = columns_of_target_table
                                    .iter()
                                    .find(|col| col.position == col_cursor);

                                if current_col.is_none() {
                                    let row_count = columns_of_target_table.len();
                                    let row_len = row.len();
                                    panic!("ERROR: Shouldn't happen. Trying to read column {} of {} column row. Currently on byte {} of {}. See: {:?}", col_cursor, row_count, byte_cursor, row_len, columns_of_target_table);
                                }

                                let col_name =
                                    String::from_utf8(current_col.unwrap().name.clone()).unwrap();

                                let col_position_in_item_list = match selected_columns.iter().position(|c| c.col.name == col_name) {
                                    Some(pos) => pos,
                                    None => 0,
                                } as u8;

                                // Check if the column we're currently parsing is actually in the query select_items
                                let is_in_output =
                                    statement.select_item_list.item_list.iter().any(|item| {
                                        match &item.expr {
                                            Expr::IsTrue(expr) => todo!(),
                                            Expr::IsNotTrue(expr) => todo!(),
                                            Expr::IsFalse(expr) => todo!(),
                                            Expr::IsNotFalse(expr) => todo!(),
                                            Expr::IsNull(expr) => todo!(),
                                            Expr::IsNotNull(expr) => todo!(),
                                            Expr::IsIn { expr, list } => todo!(),
                                            Expr::IsNotIn { expr, list } => todo!(),
                                            Expr::Between {
                                                expr,
                                                lower,
                                                higher,
                                            } => todo!(),
                                            Expr::NotBetween {
                                                expr,
                                                lower,
                                                higher,
                                            } => todo!(),
                                            Expr::Like { expr, pattern } => todo!(),
                                            Expr::NotLike { expr, pattern } => todo!(),
                                            Expr::BinaryOperator { left, op, right } => todo!(),
                                            Expr::Value(_) => false,
                                            Expr::Identifier(identifier) => identifier.value == col_name,
                                            Expr::QualifiedIdentifier(ident) => ident.identifier.value == col_name,
                                            Expr::Wildcard => true,
                                        }
                                    });

                                let col_len = match current_col.unwrap().data_type {
                                    ColumnType::Bit => 1,
                                    ColumnType::Byte => 1,
                                    ColumnType::Short => 2,
                                    ColumnType::Int => 4,
                                    ColumnType::Long => 8,
                                    ColumnType::String => {
                                        // If we hit a string column, we expect a length followed by the value of that length.
                                        // TODO: update the length to not be 1 byte. this is terrible.
                                        let len = row[byte_cursor] as usize;
                                        byte_cursor += 1;
                                        len
                                    }
                                    ColumnType::Boolean => 1,
                                    ColumnType::Date => 2,
                                    ColumnType::DateTime => 4,
                                };

                                if is_in_output {
                                    log::trace!("Reading column {:?}, starting at byte pos {} with len {}", col_name, byte_cursor, col_len);

                                    let col_bytes = &row[byte_cursor..(byte_cursor + col_len)];

                                    // TODO: format ExprResults
                                    let expr = match current_col.unwrap().data_type {
                                        ColumnType::Bit => todo!(),
                                        ColumnType::Byte => ExprResult::Byte(col_bytes[0]), // should only be 1 byte in the slice
                                        ColumnType::Short => ExprResult::Short(u16::from_be_bytes(
                                            col_bytes.try_into().unwrap(),
                                        )),
                                        ColumnType::Int => ExprResult::Int(i32::from_be_bytes(
                                            col_bytes.try_into().unwrap(),
                                        )),
                                        ColumnType::Long => ExprResult::Long(i64::from_be_bytes(
                                            col_bytes.try_into().unwrap(),
                                        )),
                                        ColumnType::String => ExprResult::String(
                                            String::from_utf8_lossy(col_bytes).to_string(),
                                        ),
                                        ColumnType::Boolean => ExprResult::Bool(col_bytes[0] == 0x1), // TODO: Not sure if this is correct
                                        ColumnType::Date => ExprResult::String(String::from("")), // TODO ExprResult::String(into_time(col_bytes)),
                                        ColumnType::DateTime => todo!(),
                                    };

                                    log::trace!("Parsed value: {:?} = {:?}", col_name, expr);

                                    results.push(ExprResultWithPosition { pos: col_position_in_item_list, expr });
                                }

                                col_cursor += 1;
                                byte_cursor += col_len;
                            }

                            log::trace!("Parsed row columns: {:?}", results);

                            results
                        })
                        .collect();

                let sorted_rows = results_with_positions
                    .iter_mut()
                    .map(|r| {
                        r.sort_by(|a, b| a.pos.cmp(&b.pos));

                        // TODO: clone
                        r.into_iter().map(|e| e.expr.clone()).collect()
                    })
                    .collect();

                // TODO: clone
                let sorted_columns = selected_columns.iter().map(|c| c.col.clone()).collect();

                // TODO: Group By, Order By, Where

                let result_set = ResultSet {
                    columns: sorted_columns,
                    rows: sorted_rows,
                };

                Ok(StatementResult { result_set })
            }
            None => Err(SelectStatementError::NonConstantExprNoFrom.into()),
        }
    }

    fn evaluate_column_name(&self, identifier: &Option<Identifier>, index: usize) -> String {
        match identifier {
            Some(id) => id.value.to_string(),
            None => String::from("Column ") + &index.to_string(),
        }
    }

    fn evaluate_constant_expr(&self, expr: &Expr) -> ExprResult {
        match expr {
            Expr::Value(value) => self.evaluate_value(value),
            Expr::IsTrue(_) => todo!(),
            Expr::IsNotTrue(_) => todo!(),
            Expr::IsFalse(_) => todo!(),
            Expr::IsNotFalse(_) => todo!(),
            Expr::IsNull(_) => todo!(),
            Expr::IsNotNull(_) => todo!(),
            Expr::IsIn { expr, list } => todo!(),
            Expr::IsNotIn { expr, list } => todo!(),
            Expr::Between {
                expr,
                lower,
                higher,
            } => todo!(),
            Expr::NotBetween {
                expr,
                lower,
                higher,
            } => todo!(),
            Expr::Like { expr, pattern } => todo!(),
            Expr::NotLike { expr, pattern } => todo!(),
            Expr::BinaryOperator { left, op, right } => match op {
                parser::ast::BinaryOperator::Plus => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Null;
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Int(l + r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Byte(l + r),
                        (ExprResult::String(l), ExprResult::String(r)) => {
                            ExprResult::String(format!("{}{}", l, r))
                        }
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::Minus => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Null;
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Int(l - r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Byte(l - r),
                        // Cannot negate strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::Multiply => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Null;
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Int(l * r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Byte(l * r),
                        // Cannot multiply strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::Divide => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Null;
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => {
                            if r == 0 {
                                ExprResult::Int(0)
                            } else {
                                ExprResult::Int(l / r)
                            }
                        }
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => {
                            if r == 0 {
                                ExprResult::Byte(0)
                            } else {
                                ExprResult::Byte(l / r)
                            }
                        }
                        // Cannot divide strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::Modulo => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Null;
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Int(l % r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Byte(l % r),
                        // Cannot modulo strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::GreaterThan => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Bool(false);
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Bool(l > r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Bool(l > r),
                        // Cannot compare strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::GreaterThanOrEqual => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Bool(false);
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Bool(l >= r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Bool(l >= r),
                        // Cannot compare strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::LessThan => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Bool(false);
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Bool(l < r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Bool(l < r),
                        // Cannot compare strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::LessThanOrEqual => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Bool(false);
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Bool(l <= r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Bool(l <= r),
                        // Cannot compare strings
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::Equal => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Bool(false);
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Bool(l == r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Bool(l == r),
                        (ExprResult::String(l), ExprResult::String(r)) => ExprResult::Bool(l == r),
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::NotEqual => {
                    let left = self.evaluate_constant_expr(left);
                    let right = self.evaluate_constant_expr(right);

                    if left == ExprResult::Null || right == ExprResult::Null {
                        return ExprResult::Bool(false);
                    }

                    match (left, right) {
                        (ExprResult::Int(l), ExprResult::Int(r)) => ExprResult::Bool(l != r),
                        (ExprResult::Byte(l), ExprResult::Byte(r)) => ExprResult::Bool(l != r),
                        (ExprResult::String(l), ExprResult::String(r)) => ExprResult::Bool(l != r),
                        _ => ExprResult::Null,
                    }
                }
                parser::ast::BinaryOperator::And => todo!(),
                parser::ast::BinaryOperator::Or => todo!(),
                parser::ast::BinaryOperator::Xor => todo!(),
                parser::ast::BinaryOperator::BitwiseOr => todo!(),
                parser::ast::BinaryOperator::BitwiseAnd => todo!(),
                parser::ast::BinaryOperator::BitwiseXor => todo!(),
            },
            Expr::Identifier(_) => todo!(),
            Expr::QualifiedIdentifier(_) => todo!(),
            Expr::Wildcard => todo!(),
        }
    }

    fn evaluate_value(&self, value: &Value) -> ExprResult {
        match value {
            Value::Number(n) => self.evaluate_number(n),
            Value::String(s, _quote_type) => ExprResult::String(s.to_string()),
            Value::Boolean(b) => ExprResult::Bool(*b),
            Value::Null => ExprResult::Null,
        }
    }

    fn evaluate_number(&self, number: &str) -> ExprResult {
        if let Ok(parse) = number.parse() {
            return ExprResult::Int(parse);
        }

        ExprResult::Null
    }
}
