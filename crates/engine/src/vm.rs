use anyhow::{Error, Result};
use deku::DekuReader;
use derive_more::derive::From;
use parser::ast::{Expr, Identifier, SelectExpressionBody, SelectItem, UserStatement, Value};
use std::{cell::RefCell, rc::Rc};
use thiserror::Error;

use crate::{
    db::{FileType, SchemaInfo, SCHEMA_INFO_PAGE_INDEX},
    engine::{ColumnResult, ExprResult, ResultSet, StatementResult},
    fm::FileManager,
    index_pager::IndexPager,
    page::{PageDecoder, PageId},
    page_cache::FilePageId,
    persistence,
    server::{Column, ColumnType, Database, Index, IndexType, Table, MASTER_DB_ID},
};

pub struct VirtualMachine {
    file_manager: Rc<RefCell<FileManager>>,
    index_pager: Rc<RefCell<IndexPager>>,
}

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

impl VirtualMachine {
    pub fn new(
        file_manager: Rc<RefCell<FileManager>>,
        index_pager: Rc<RefCell<IndexPager>>,
    ) -> Self {
        VirtualMachine {
            file_manager,
            index_pager,
        }
    }

    pub fn execute_user_statement(&self, statement: &UserStatement) -> Result<StatementResult> {
        let is_const_expr = self.is_constant_statement(statement);

        if is_const_expr {
            log::debug!("Statement is constant");
            return self.evaluate_constant_statement(statement);
        }

        match statement {
            UserStatement::Select(s) => self.evaluate_select_statement(s),
            UserStatement::Update => todo!(),
            UserStatement::Insert => todo!(),
            UserStatement::Delete => todo!(),
            UserStatement::CreateTable(_) => todo!(),
        }
    }

    // todo: type?
    fn is_constant_statement(&self, statement: &UserStatement) -> bool {
        match statement {
            UserStatement::Select(select_expression_body) => select_expression_body
                .select_item_list
                .item_list
                .iter()
                .all(|item| self.is_const_exp(&item.expr)),
            UserStatement::Update => todo!(),
            UserStatement::Insert => todo!(),
            UserStatement::Delete => todo!(),
            UserStatement::CreateTable(_) => todo!(),
        }
    }

    fn is_const_exp(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Between {
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
            Expr::IsIn { expr, list } => {
                self.is_const_exp(expr) && list.iter().all(|e| self.is_const_exp(e))
            }
            Expr::IsNotFalse(expr) => self.is_const_exp(expr),
            Expr::IsNotTrue(expr) => self.is_const_exp(expr),
            Expr::Value(_) => true,
            Expr::Identifier(_) => false,
            _ => false,
        }
    }

    fn evaluate_constant_statement(&self, statement: &UserStatement) -> Result<StatementResult> {
        match statement {
            UserStatement::Select(select_expression_body) => {
                let columns = select_expression_body
                    .select_item_list
                    .item_list
                    .iter()
                    .enumerate()
                    .map(|(index, item)| ColumnResult {
                        name: self.evaluate_column_name(&item.alias, index),
                        value: self.evaluate_constant_expr(&item.expr),
                    })
                    .collect();

                Ok(StatementResult {
                    result_set: ResultSet { columns },
                })
            }
            UserStatement::Update => todo!(),
            UserStatement::Insert => todo!(),
            UserStatement::Delete => todo!(),
            UserStatement::CreateTable(_) => todo!(),
        }
    }

    // TODO: refactor this monster
    fn evaluate_select_statement(
        &self,
        statement: &SelectExpressionBody,
    ) -> Result<StatementResult> {
        log::debug!("SELECT:");
        match &statement.from_clause {
            Some(from) => {
                // TODO: this check if very simple (not fully correct)
                let is_select_wildcard =
                    statement.select_item_list.item_list[0] == SelectItem::new(Expr::Wildcard);

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
                let fm = self.file_manager.borrow();
                let master_data_file = fm.get_from_id(MASTER_DB_ID, FileType::Primary);

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

                let pager = self.index_pager.borrow();

                // Step 3.
                // Create an pager and read all the Database records that exist
                // in the schema.
                // Validate if the database exists.
                let databases_page_iter = pager.create_pager(FilePageId::new(
                    MASTER_DB_ID,
                    schema_info.databases_root_page_id,
                ));

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
                let tables_page_iter = pager.create_pager(FilePageId::new(
                    MASTER_DB_ID,
                    schema_info.tables_root_page_id,
                ));

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
                let indexes_page_iter = pager.create_pager(FilePageId::new(
                    MASTER_DB_ID,
                    schema_info.indexes_root_page_id,
                ));

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

                let target_table_index_root_id: PageId = pk_index_for_target_table.root_page_id;

                log::debug!("Found PK index for table {}.", &table_name);

                // Step 6.
                // Create a pager and read all Columns from the Schema that exist.
                // Validate that the requested columns exist.
                let columns_page_iter = pager.create_pager(FilePageId::new(
                    MASTER_DB_ID,
                    schema_info.columns_root_page_id,
                ));

                let mut columns = columns_page_iter.map(|item| {
                    let mut cursor = std::io::Cursor::new(item);
                    let mut reader = deku::reader::Reader::new(&mut cursor);

                    return Column::from_reader_with_ctx(&mut reader, ());
                });

                let mut columns_of_target_table =
                    columns.filter(|c| c.as_ref().is_ok_and(|f| f.table_id == target_table_id));

                // Step 6.5.
                // Validate that the requested columns exist.
                // TODO: Handle all types of SelectItem expressions.
                if !is_select_wildcard {
                    let missing_columns: Vec<SelectItem> = statement
                        .select_item_list
                        .item_list
                        .iter()
                        .cloned()
                        .filter(|i| match &i.expr {
                            Expr::Identifier(ident) => columns_of_target_table.any(|c| {
                                String::from_utf8(c.unwrap().name).unwrap() == ident.value
                            }),
                            _ => true,
                        })
                        .collect();

                    if missing_columns.len() > 0 {
                        return Err(StatementError::ColumnsDoNotExist(missing_columns).into());
                    }
                }

                // Step 7.
                // Because deku wont work to deserialise a type it knows nothing about,
                // we need to use our column schema info to decide how to read the incoming row bytes.
                let mut target_table_columns: Vec<(u16, u8, ColumnType)> = columns_of_target_table
                    .map(|c| {
                        let col = c.unwrap();
                        let column_size = match col.data_type {
                            crate::server::ColumnType::Bit => 1,
                            crate::server::ColumnType::Byte => 1,
                            crate::server::ColumnType::Int => 2,
                            crate::server::ColumnType::String => 0, // TODO: I have no idea how to know this... There's a len byte somewhere in the Vec<u8> ha
                            crate::server::ColumnType::Boolean => 1,
                            crate::server::ColumnType::Date => 2, // TODO: confirm
                            crate::server::ColumnType::DateTime => 4, // TODO: confirm
                        };

                        // TODO: will probably need name back too (though maybe not from here -
                        // after all, the name is the same for all rows, we can probs do it based on pos)

                        (column_size, col.position, col.data_type)
                    })
                    .collect();

                // TODO: use struct/enum instead of tuple so it's not such jank syntax
                target_table_columns.sort_by(|a, b| a.1.cmp(&b.1));

                // Step 8.
                // Create a pager and read all data from the target table.
                // This is more complex than before, as we can't rely on deku to parse the
                // Vec<u8> into a target type - we don't know the type!
                let target_table_iter =
                    pager.create_pager(FilePageId::new(MASTER_DB_ID, target_table_index_root_id));

                let target_table_data: Vec<Vec<Vec<u8>>> = target_table_iter
                    .map(|item| {
                        let mut cursor: u16 = 0;
                        let mut cols: Vec<Vec<u8>> = Vec::new();

                        log::debug!("Processing row {:?}", item);
                        for (col_length, _col_pos, col_type) in &target_table_columns {
                            log::debug!("  > Reading col {:?}", _col_pos);
                            let col_bytes = item[cursor.into()..(*col_length).into()].to_vec();
                            cols.push(col_bytes);
                            cursor = cursor + col_length;
                        }

                        cols
                    })
                    .collect();

                log::debug!("{:?}", target_table_data);

                // TODO: Group By, Order By, Where

                Ok(StatementResult {
                    result_set: ResultSet { columns: vec![] },
                })
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
