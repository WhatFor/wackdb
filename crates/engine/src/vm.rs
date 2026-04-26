use anyhow::{bail, Result};
use cli_common::{ColumnResult, ExprResult, ResultSet, StatementResult};
use derive_more::derive::From;
use parser::ast::{
    Expr, Identifier, InsertExpressionBody, SelectExpressionBody, SelectItem, Statement, Value,
};
use thiserror::Error;

use crate::{
    buffer_pool::FilePageId,
    catalog::{ColumnType, DbLong, IndexType, MASTER_DB_ID},
    engine::Storage,
    index_pager::IndexPager,
    sm::{SchemaColumn, SchemaManager},
    wal::{LogType, WalLog},
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
        storage: &Storage,
        schema: &SchemaManager,
    ) -> Result<StatementResult> {
        let is_const_expr = self.is_constant_statement(statement);

        if is_const_expr {
            log::debug!("Statement is constant");
            return self.evaluate_constant_statement(statement);
        }

        match statement {
            Statement::Select(select_statement) => {
                self.evaluate_select_statement(select_statement, storage, schema)
            }
            Statement::Update => todo!(),
            Statement::Insert(insert_statement) => {
                self.evaluate_insert_statement(insert_statement, storage, schema)
            }
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
            Statement::Update => false,
            Statement::Insert(_) => false,
            Statement::Delete => false,
            Statement::CreateTable(_) => false,
            Statement::CreateDatabase(_) => false,
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
            Statement::Update => unreachable!("UPDATE statements are never constant."),
            Statement::Insert(_) => unreachable!("INSERT statements are never constant."),
            Statement::Delete => unreachable!("DELETE statements are never constant."),
            Statement::CreateTable(_) => {
                unreachable!("CREATE TABLE statements are never constant.")
            }
            Statement::CreateDatabase(_) => {
                unreachable!("CREATE DATABASE statements are never constant.")
            }
        }
    }

    // TODO: refactor this monster
    fn evaluate_select_statement(
        &self,
        statement: &SelectExpressionBody,
        storage: &Storage,
        sm: &SchemaManager,
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
                // Validate that the database we're reading from exists
                let target_db = sm.schema.databases.iter().find(|i| i.name == database_name);

                if target_db.is_none() {
                    return Err(StatementError::DbDoesNotExist.into());
                }

                log::debug!("Validated database {} exists.", &database_name);

                // Step 2.
                // Validate if the target table exists.
                let target_table = target_db
                    .unwrap() // TODO: Unwrap (is safe, just pointless having to unwrap)
                    .tables
                    .iter()
                    .find(|t| t.name == *table_name);

                if target_table.is_none() {
                    return Err(StatementError::TableDoesNotExist.into());
                }

                // TODO: unwrap is pointless
                let target_table_id = target_table.unwrap().id;

                log::debug!("Validated table {} exists.", &table_name);

                // Step 3.
                // Find the Primary Key index on the target table.
                // This is needed to know where to start reading the data from.
                let pk_index_for_target_table = target_table
                    .unwrap() // TODO: pointless unwrap, we know it exists
                    .indexes
                    .iter()
                    .find(|i| i.table_id == target_table_id && i.index_type == IndexType::PK)
                    .unwrap(); // TODO: handle

                let target_table_index_root_id: DbLong = pk_index_for_target_table.root_page_id;

                log::debug!("Found PK index for table {}.", &table_name);

                // Step 4.
                // Validate that the requested columns exist.
                let mut columns_of_target_table: Vec<&SchemaColumn> = target_table
                    .unwrap() // Pointless unwrap
                    .columns
                    .iter()
                    .filter(|c| c.table_id == target_table_id)
                    .collect();

                columns_of_target_table.sort_by(|a, b| a.position.cmp(&b.position));

                // Find the columns we need to process in the query.
                //      If wildcard, will be all columns sorted by the position property from master.columns,
                //      else will be sorted in the order in which the columns appeared in the query.
                let selected_columns: Vec<ColumnResultWithMetadata> = if is_select_wildcard {
                    // TODO: This needs work. It just dumps all columns as our selected, but doesn't account for const columns.
                    // Test: SELECT *, 'Hello' from Tables
                    columns_of_target_table
                        .iter()
                        .map(|col| ColumnResultWithMetadata {
                            col: ColumnResult {
                                name: col.name.clone(),
                                alias: None,
                            },
                            pos: col.position,
                            const_value: None,
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

                // Step 5.
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
                                .all(|c| c.name != ident.identifier.value),
                            Expr::Identifier(ident) => columns_of_target_table
                                .iter()
                                .all(|c| c.name != ident.value),
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

                // Step 6.
                // Because deku wont work to deserialise a type it knows nothing about,
                // we need to use our column schema info to decide how to read the incoming row bytes.
                let target_table_iter = IndexPager::new(
                    FilePageId::new(MASTER_DB_ID, target_table_index_root_id as u32), // TODO: if this was a user DB, it fails
                    storage,
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

                                let col_name = current_col.unwrap().name.clone();

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

    fn evaluate_insert_statement(
        &self,
        statement: &InsertExpressionBody,
        storage: &Storage,
        sm: &SchemaManager,
    ) -> Result<StatementResult> {
        // Step 1.
        // Verify the database exists.
        let table_name = &statement.into_clause.identifier.value;
        let is_qualified = statement.into_clause.qualifier.is_some();

        match is_qualified {
            true => log::debug!(
                "   FROM: {}.{}",
                &statement.into_clause.qualifier.as_ref().unwrap(),
                table_name
            ),
            false => log::debug!("   FROM: {}", table_name),
        }

        // TODO: this is me just falling back to the master database if the user doesn't specify a db. not a final solution...
        let database_name = if is_qualified {
            statement
                .into_clause
                .qualifier
                .as_ref()
                .unwrap()
                .value
                .clone()
        } else {
            String::from("master")
        };

        let target_db = sm.schema.databases.iter().find(|i| i.name == database_name);

        if target_db.is_none() {
            return Err(StatementError::DbDoesNotExist.into());
        }

        // Step 2.
        // Validate if the target table exists.
        let target_table = target_db
            .unwrap() // TODO: Unwrap (is safe, just pointless having to unwrap)
            .tables
            .iter()
            .find(|t| t.name == *table_name);

        if target_table.is_none() {
            return Err(StatementError::TableDoesNotExist.into());
        }

        let target_table_id = target_table.unwrap().id;

        // 2. Verify the columns exist
        let columns_of_target_table: Vec<&SchemaColumn> = target_table
            .unwrap() // Pointless unwrap
            .columns
            .iter()
            .filter(|c| c.table_id == target_table_id)
            .collect();

        let selected_columns: Vec<&Identifier> = statement
            .column_list
            .column_list
            .iter()
            .map(|col| &col.ident)
            .collect();

        let missing_columns: Vec<&Identifier> = selected_columns
            .iter()
            .filter(|col| columns_of_target_table.iter().all(|c| c.name != col.value))
            .cloned()
            .collect();

        if !missing_columns.is_empty() {
            log::trace!("[EVAL SELECT] Missing columns: {:?}", missing_columns);
            bail!("TODO: Better error (but for now, columns are missing in your query).");
        }

        // TODO: Need to actual compute the insert, because we need:
        //   which page we're operating on
        //      so we can store this in the log payload (along with the actual data)
        //      and so we can make it dirty in the buffer_pool
        //   which slot in the page we're operating on
        // which means...
        //   we probalby need to ask the schema where the index is for the table
        //   we're operating on, because the data will get stored in a leaf node
        //   of the PK table.
        let pk_index = target_table
            .unwrap() // TODO: Safe upwrap; handle it above
            .indexes
            .iter()
            .find(|i| i.index_type == IndexType::PK);

        if pk_index.is_none() {
            bail!("TODO: For now, can't insert into a table without a PK.");
        }

        let pk_index_pager = IndexPager::new(
            FilePageId {
                db_id: MASTER_DB_ID, // TODO: need to support user DBs
                page_index: pk_index.unwrap().root_page_id as u32,
            },
            &storage,
        );

        // TODO: I have no idea what to do with the PK; do I need to reconstruct
        // the btree and add it in? christ I've no idea

        // Step 3.
        // Record the data in the WAL.
        let payload = vec![];
        let log = WalLog::new(0, None, None, LogType::Insert, payload);

        storage
            .wal
            .log(&storage.file_manager, &(target_db.unwrap().id as u16), log)?;

        // Step 4.
        // Add the new data to the buffer_pool.

        todo!()
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

#[cfg(test)]
mod vm_tests {

    use super::*;
    use crate::{
        buffer_pool::BufferPool,
        fm::FileManager,
        sm::{Schema, SchemaDatabase, SchemaTable},
        wal::Wal,
    };
    use anyhow::Result;
    use parser::ast::*;

    fn create_test_storage() -> Result<Storage> {
        let file_manager = FileManager::default();
        let wal = Wal::default();

        Ok(Storage {
            buffer_pool: BufferPool::default(),
            file_manager,
            wal,
        })
    }

    fn create_test_schema() -> SchemaManager {
        SchemaManager::from(Schema {
            databases: vec![SchemaDatabase {
                id: 0,
                name: String::from("TEST"),
                tables: vec![SchemaTable {
                    id: 0,
                    name: String::from("Table"),
                    database_id: 0,
                    columns: vec![],
                    indexes: vec![],
                }],
            }],
        })
    }

    #[test]
    /// SELECT 1;
    fn test_constant_select_integer() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let one = Expr::Value(Value::Number("1".to_string()));

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(one)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(1));
        Ok(())
    }

    #[test]
    /// SELECT 1, 2, 3;
    fn test_constant_select_integer_multiple_columns() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let one = Expr::Value(Value::Number("1".to_string()));
        let two = Expr::Value(Value::Number("2".to_string()));
        let three = Expr::Value(Value::Number("3".to_string()));

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![
                SelectItem::new(one),
                SelectItem::new(two),
                SelectItem::new(three),
            ]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(1));
        assert_eq!(result.result_set.rows[0][1], ExprResult::Int(2));
        assert_eq!(result.result_set.rows[0][2], ExprResult::Int(3));
        Ok(())
    }

    #[test]
    /// SELECT 1 As AliasName;
    fn test_constant_select_integer_with_alias() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let value = Expr::Value(Value::Number("1".to_string()));
        let alias = Identifier {
            value: String::from("AliasName"),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::aliased(value, alias)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(1));
        assert_eq!(
            result.result_set.columns[0],
            ColumnResult {
                name: String::from("AliasName"),
                alias: Some(String::from("AliasName"))
            }
        );
        Ok(())
    }

    #[test]
    /// SELECT 1 AS A, 2 AS B, 3 AS C;
    fn test_constant_select_integer_multiple_columns_with_aliases() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let one = Expr::Value(Value::Number("1".to_string()));
        let one_alias = Identifier::from(String::from("A"));

        let two = Expr::Value(Value::Number("2".to_string()));
        let two_alias = Identifier::from(String::from("B"));

        let three = Expr::Value(Value::Number("3".to_string()));
        let three_alias = Identifier::from(String::from("C"));

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![
                SelectItem::aliased(one, one_alias),
                SelectItem::aliased(two, two_alias),
                SelectItem::aliased(three, three_alias),
            ]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(
            result.result_set.columns[0],
            ColumnResult {
                name: String::from("A"),
                alias: Some(String::from("A"))
            }
        );
        assert_eq!(
            result.result_set.columns[1],
            ColumnResult {
                name: String::from("B"),
                alias: Some(String::from("B"))
            }
        );
        assert_eq!(
            result.result_set.columns[2],
            ColumnResult {
                name: String::from("C"),
                alias: Some(String::from("C"))
            }
        );
        Ok(())
    }

    #[test]
    /// SELECT 1 + 2;
    fn test_constant_select_add() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Plus,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(3));
        Ok(())
    }

    #[test]
    /// SELECT 3 - 2;
    fn test_constant_select_subtract() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("3".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Minus,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(1));
        Ok(())
    }

    #[test]
    /// SELECT 3 * 4;
    fn test_constant_select_multiply() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("3".to_string()));
        let right = Expr::Value(Value::Number("4".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Multiply,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(12));
        Ok(())
    }

    #[test]
    /// SELECT 12 / 4;
    fn test_constant_select_divide_whole_number() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("12".to_string()));
        let right = Expr::Value(Value::Number("4".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Divide,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(3));
        Ok(())
    }

    #[test]
    /// SELECT 1 / 0;
    /// TODO: Should blow up, but currently just returns 0. That's not correct.
    fn test_constant_select_divide_by_zero() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("0".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Divide,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(0));
        Ok(())
    }

    #[test]
    /// SELECT 11 % 5;
    fn test_constant_select_modulo() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("11".to_string()));
        let right = Expr::Value(Value::Number("5".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Modulo,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Int(1));
        Ok(())
    }

    #[test]
    /// SELECT 'Hello';
    fn test_constant_select_string() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let str = Expr::Value(Value::String(String::from("Hello"), QuoteType::Single));

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(str)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(
            result.result_set.rows[0][0],
            ExprResult::String(String::from("Hello"))
        );
        Ok(())
    }

    #[test]
    /// SELECT 'Hello, ' + 'World';
    fn test_constant_select_string_concat() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::String(String::from("Hello, "), QuoteType::Single));
        let right = Expr::Value(Value::String(String::from("World"), QuoteType::Single));
        let concat = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Plus,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(concat)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(
            result.result_set.rows[0][0],
            ExprResult::String(String::from("Hello, World"))
        );
        Ok(())
    }

    #[test]
    /// SELECT NULL;
    fn test_constant_select_null() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let null = Expr::Value(Value::Null);

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(null)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Null);
        Ok(())
    }

    #[test]
    /// SELECT 1 + NULL;
    fn test_constant_select_numeric_plus_null_returns_null() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("5".to_string()));
        let right = Expr::Value(Value::Null);
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Plus,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Null);
        Ok(())
    }

    #[test]
    /// SELECT 1 + 'Hello';
    /// TODO: This is probably a bit odd; It should return an error.
    fn test_constant_select_numeric_plus_string_returns_null() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("5".to_string()));
        let right = Expr::Value(Value::String(String::from("Hello"), QuoteType::Single));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Plus,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Null);
        Ok(())
    }

    #[test]
    /// SELECT 2 > 1;
    fn test_constant_select_number_greater_than_number_true() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("2".to_string()));
        let right = Expr::Value(Value::Number("1".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::GreaterThan,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }

    #[test]
    /// SELECT 1 > 2;
    fn test_constant_select_number_greater_than_number_false() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::GreaterThan,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(false));
        Ok(())
    }

    #[test]
    /// SELECT 2 >= 2;
    fn test_constant_select_number_greater_than_or_equal_number_same_value() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("2".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::GreaterThanOrEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }

    #[test]
    /// SELECT 3 >= 2;
    fn test_constant_select_number_greater_than_or_equal_number_true() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("3".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::GreaterThanOrEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }

    #[test]
    /// SELECT 2 >= 3;
    fn test_constant_select_number_greater_than_or_equal_number_false() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("2".to_string()));
        let right = Expr::Value(Value::Number("3".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::GreaterThanOrEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(false));
        Ok(())
    }

    #[test]
    /// SELECT 2 < 1;
    fn test_constant_select_number_less_than_number_false() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("2".to_string()));
        let right = Expr::Value(Value::Number("1".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::LessThan,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(false));
        Ok(())
    }

    #[test]
    /// SELECT 1 < 2;
    fn test_constant_select_number_less_than_number_true() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::LessThan,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }

    #[test]
    /// SELECT 2 <= 2;
    fn test_constant_select_number_less_than_or_equal_number_same_value() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("2".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::LessThanOrEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }

    #[test]
    /// SELECT 3 <= 2;
    fn test_constant_select_number_less_than_or_equal_number_false() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("3".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::LessThanOrEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(false));
        Ok(())
    }

    #[test]
    /// SELECT 2 <= 3;
    fn test_constant_select_number_less_than_or_equal_number_true() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("2".to_string()));
        let right = Expr::Value(Value::Number("3".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::LessThanOrEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }

    #[test]
    /// SELECT 1 = 1;
    fn test_constant_select_number_equal_number_true() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("1".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Equal,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }

    #[test]
    /// SELECT 1 = 2;
    fn test_constant_select_number_equal_number_false() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::Equal,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(false));
        Ok(())
    }

    #[test]
    /// SELECT 1 <> 1;
    fn test_constant_select_number_not_equal_number_false() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("1".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::NotEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(false));
        Ok(())
    }

    #[test]
    /// SELECT 1 <> 2;
    fn test_constant_select_number_not_equal_number_true() -> Result<()> {
        let vm = VirtualMachine::default();
        let storage = create_test_storage()?;
        let sm = create_test_schema();

        let left = Expr::Value(Value::Number("1".to_string()));
        let right = Expr::Value(Value::Number("2".to_string()));
        let op = Expr::BinaryOperator {
            left: left.into(),
            op: BinaryOperator::NotEqual,
            right: right.into(),
        };

        let stmt = Statement::Select(SelectExpressionBody {
            select_item_list: SelectItemList::from(vec![SelectItem::new(op)]),
            from_clause: None,
            where_clause: None,
            order_by_clause: None,
            group_by_clause: None,
        });

        let result = vm.execute_statement(&stmt, &storage, &sm).unwrap();

        assert_eq!(result.result_set.rows[0][0], ExprResult::Bool(true));
        Ok(())
    }
}
