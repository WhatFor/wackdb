#![allow(unused_variables)]

use anyhow::Result;
use derive_more::derive::From;
use parser::ast::{Expr, Identifier, SelectExpressionBody, SelectItem, UserStatement, Value};
use thiserror::Error;

use crate::engine::{ColumnResult, ExprResult, ResultSet, StatementResult};

pub fn execute_user_statement(statement: &UserStatement) -> Result<StatementResult> {
    let is_const_expr = is_constant_statement(statement);

    if is_const_expr {
        log::debug!("Statement is constant");
        return evaluate_constant_statement(statement);
    }

    match statement {
        UserStatement::Select(s) => evaluate_select_statement(s),
        UserStatement::Update => todo!(),
        UserStatement::Insert => todo!(),
        UserStatement::Delete => todo!(),
        UserStatement::CreateTable(_) => todo!(),
    }
}

// todo: type?
fn is_constant_statement(statement: &UserStatement) -> bool {
    match statement {
        UserStatement::Select(select_expression_body) => select_expression_body
            .select_item_list
            .item_list
            .iter()
            .all(|item| is_const_exp(&item.expr)),
        UserStatement::Update => todo!(),
        UserStatement::Insert => todo!(),
        UserStatement::Delete => todo!(),
        UserStatement::CreateTable(_) => todo!(),
    }
}

fn is_const_exp(expr: &Expr) -> bool {
    match expr {
        Expr::Between {
            expr,
            lower,
            higher,
        } => is_const_exp(expr) && is_const_exp(lower) && is_const_exp(higher),
        Expr::BinaryOperator { left, right, .. } => is_const_exp(left) && is_const_exp(right),
        Expr::IsFalse(expr) => is_const_exp(expr),
        Expr::IsTrue(expr) => is_const_exp(expr),
        Expr::IsNull(expr) => is_const_exp(expr),
        Expr::IsNotNull(expr) => is_const_exp(expr),
        Expr::Like { expr, pattern } => is_const_exp(expr) && is_const_exp(pattern),
        Expr::IsIn { expr, list } => is_const_exp(expr) && list.iter().all(is_const_exp),
        Expr::IsNotFalse(expr) => is_const_exp(expr),
        Expr::IsNotTrue(expr) => is_const_exp(expr),
        Expr::Value(_) => true,
        Expr::Identifier(_) => false,
        _ => false,
    }
}

fn evaluate_constant_statement(statement: &UserStatement) -> Result<StatementResult> {
    match statement {
        UserStatement::Select(select_expression_body) => {
            let columns = select_expression_body
                .select_item_list
                .item_list
                .iter()
                .enumerate()
                .map(|(index, item)| ColumnResult {
                    name: evaluate_column_name(&item.alias, index),
                    value: evaluate_constant_expr(&item.expr),
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

#[derive(Debug, From, Error)]
enum SelectStatementError {
    #[error("Non-constant query contains no 'FROM' expression.")]
    NonConstantExprNoFrom,
}

fn evaluate_select_statement(statement: &SelectExpressionBody) -> Result<StatementResult> {
    log::debug!("SELECT:");
    match &statement.from_clause {
        Some(from) => {
            let is_select_wildcard =
                statement.select_item_list.item_list[0] == SelectItem::new(Expr::Wildcard);
            log::debug!("   ITEMS: {:?}", statement.select_item_list.item_list);

            let table_name = &from.identifier.value;
            let is_qualified = &from.qualifier.is_some();

            match is_qualified {
                true => log::debug!(
                    "   FROM: {}.{}",
                    &from.qualifier.as_ref().unwrap(),
                    table_name
                ),
                false => log::debug!("   FROM: {}", table_name),
            }

            // we need to find the table we're asking for. we have it's identifier, so...
            // ideally the schema info should already be in memory, but we're going to do it from disk as a POC here.
            // 1. read schema page to get root index page IDs.
            // 2. read the root page of the databases index and check the db we're querying exists.
            // 3. if not, error
            // 4. read the root page of the tables index and check the table exists.
            // 5. if not, error
            // 6. from there, get the root of the table index
            // 7. read the data (???)
            // 8. return the data

            // TODO: Group By, Order By, Where

            Ok(StatementResult {
                result_set: ResultSet { columns: vec![] },
            })
        }
        None => Err(SelectStatementError::NonConstantExprNoFrom.into()),
    }
}

fn evaluate_column_name(identifier: &Option<Identifier>, index: usize) -> String {
    match identifier {
        Some(id) => id.value.to_string(),
        None => String::from("Column ") + &index.to_string(),
    }
}

fn evaluate_constant_expr(expr: &Expr) -> ExprResult {
    match expr {
        Expr::Value(value) => evaluate_value(value),
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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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
                let left = evaluate_constant_expr(left);
                let right = evaluate_constant_expr(right);

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

fn evaluate_value(value: &Value) -> ExprResult {
    match value {
        Value::Number(n) => evaluate_number(n),
        Value::String(s, _quote_type) => ExprResult::String(s.to_string()),
        Value::Boolean(b) => ExprResult::Bool(*b),
        Value::Null => ExprResult::Null,
    }
}

fn evaluate_number(number: &str) -> ExprResult {
    if let Ok(parse) = number.parse() {
        return ExprResult::Int(parse);
    }

    ExprResult::Null
}
