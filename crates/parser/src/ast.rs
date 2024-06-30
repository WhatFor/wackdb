use std::fmt;

#[derive(PartialEq, Debug)]
pub enum Program {
    Statements(Vec<Statement>),
    Empty,
}

#[derive(PartialEq, Debug)]
pub enum Statement {
    User(UserStatement),
    Server(ServerStatement),
}

#[derive(PartialEq, Debug)]
pub enum UserStatement {
    Select(SelectExpressionBody),
    Update,
    Insert,
    Delete,
    CreateTable(CreateTableBody),
}

#[derive(PartialEq, Debug)]
pub enum ServerStatement {
    CreateDatabase(CreateDatabaseBody),
}

#[derive(PartialEq)]
pub struct SelectExpressionBody {
    pub select_item_list: SelectItemList,
    pub from_clause: Option<FromClause>,
    pub where_clause: Option<WhereClause>,
    pub order_by_clause: Option<OrderByClause>,
    pub group_by_clause: Option<GroupByClause>,
}

#[derive(PartialEq, Debug)]
pub struct CreateTableBody {
    pub table_name: Identifier,
    pub column_list: Vec<ColumnDefinition>,
}

#[derive(PartialEq, Debug)]
pub struct ColumnDefinition {
    pub column_name: Identifier,
    pub datatype: DataType,
    pub nullable: bool,
}

#[derive(PartialEq, Debug)]
pub enum DataType {
    Int,
}

#[derive(PartialEq, Debug)]
pub struct CreateDatabaseBody {
    pub database_name: Identifier,
}

impl fmt::Display for SelectExpressionBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT {} ", self.select_item_list)?;

        match &self.from_clause {
            Some(c) => write!(f, "FROM {} ", c)?,
            _ => {}
        }

        match &self.where_clause {
            Some(c) => write!(f, "WHERE {} ", c)?,
            _ => {}
        }

        match &self.group_by_clause {
            Some(c) => write!(f, "GROUP BY {} ", c)?,
            _ => {}
        }

        match &self.order_by_clause {
            Some(c) => write!(f, "ORDER BY {}", c)?,
            _ => {}
        }

        Ok(())
    }
}

impl fmt::Debug for SelectExpressionBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub struct SelectItemList {
    pub item_list: Vec<SelectItem>,
}

impl SelectItemList {
    pub fn from(items: Vec<SelectItem>) -> Self {
        SelectItemList { item_list: items }
    }
}

impl fmt::Display for SelectItemList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.item_list)
    }
}

impl fmt::Debug for SelectItemList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<Identifier>,
}

impl fmt::Display for SelectItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;

        match &self.alias {
            Some(alias) => write!(f, " AS {}", alias),
            None => Ok(()),
        }
    }
}

impl fmt::Debug for SelectItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

impl SelectItem {
    pub fn new(expr: Expr) -> Self {
        SelectItem { expr, alias: None }
    }

    pub fn simple_identifier(identifier: &str) -> Self {
        SelectItem {
            // todo: change to identifier
            expr: Expr::Value(Value::String(String::from(identifier), QuoteType::None)),
            alias: None,
        }
    }

    pub fn qualified_identifier(identifiers: Vec<&str>) -> Self {
        let idents = identifiers
            .iter()
            .map(|i| Identifier::from(i.to_string()))
            .collect();

        SelectItem {
            expr: Expr::QualifiedIdentifier(idents),
            alias: None,
        }
    }

    pub fn aliased(expr: Expr, alias: Identifier) -> Self {
        SelectItem {
            expr,
            alias: Some(alias),
        }
    }

    pub fn aliased_identifier(identifier: &str, alias: Identifier) -> Self {
        SelectItem {
            // todo: change to identifier
            expr: Expr::Value(Value::String(String::from(identifier), QuoteType::None)),
            alias: Some(alias),
        }
    }

    pub fn aliased_qualified_identifier(identifiers: Vec<&str>, alias: Identifier) -> Self {
        let idents = identifiers
            .iter()
            .map(|i| Identifier::from(i.to_string()))
            .collect();

        SelectItem {
            expr: Expr::QualifiedIdentifier(idents),
            alias: Some(alias),
        }
    }
}

#[derive(PartialEq)]
pub struct FromClause {
    pub identifier: Identifier,
    pub alias: Option<Identifier>,
}

impl fmt::Display for FromClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.alias {
            Some(a) => write!(f, "{} AS {}", self.identifier, a),
            None => write!(f, "{}", self.identifier),
        }
    }
}

impl fmt::Debug for FromClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub struct WhereClause {
    pub expr: Expr,
}

impl fmt::Display for WhereClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)
    }
}

impl fmt::Debug for WhereClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub struct OrderByClause {
    pub identifier: Identifier,
    pub dir: OrderDirection,
}

impl fmt::Display for OrderByClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.identifier, self.dir)
    }
}

impl fmt::Debug for OrderByClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub struct GroupByClause {
    pub identifier: Identifier,
}

impl fmt::Display for GroupByClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.identifier)
    }
}

impl fmt::Debug for GroupByClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub enum Expr {
    IsTrue(Box<Expr>),
    IsNotTrue(Box<Expr>),
    IsFalse(Box<Expr>),
    IsNotFalse(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsIn {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    IsNotIn {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    Between {
        expr: Box<Expr>,
        lower: Box<Expr>,
        higher: Box<Expr>,
    },
    NotBetween {
        expr: Box<Expr>,
        lower: Box<Expr>,
        higher: Box<Expr>,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
    },
    NotLike {
        expr: Box<Expr>,
        pattern: Box<Expr>,
    },
    BinaryOperator {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    Value(Value),
    Identifier(Identifier),
    QualifiedIdentifier(Vec<Identifier>),
    Wildcard,
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::IsTrue(s) => write!(f, "{s}"),
            Expr::IsNotTrue(e) => write!(f, "{e} IS NOT TRUE"),
            Expr::IsFalse(e) => write!(f, "{e} IS FALSE"),
            Expr::IsNotFalse(e) => write!(f, "{e} IS NOT FALSE"),
            Expr::IsNull(e) => write!(f, "{e} IS NULL"),
            Expr::IsNotNull(e) => write!(f, "{e} IS NOT NULL"),
            Expr::IsIn { expr, list } => write!(f, "{expr} IS IN {list:?}"),
            Expr::IsNotIn { expr, list } => write!(f, "{expr} IS NOT IN {list:?}"),
            Expr::Between {
                expr,
                lower,
                higher,
            } => write!(f, "{expr} BETWEEN {lower} AND {higher}"),
            Expr::NotBetween {
                expr,
                lower,
                higher,
            } => write!(f, "{expr} NOT BETWEEN {lower} AND {higher}"),
            Expr::Like { expr, pattern } => write!(f, "{expr} LIKE {pattern}"),
            Expr::NotLike { expr, pattern } => write!(f, "{expr} NOT LIKE {pattern}"),
            Expr::BinaryOperator { left, op, right } => write!(f, "({left} {op} {right})"),
            Expr::Value(v) => write!(f, "{v:?}"),
            Expr::Identifier(i) => write!(f, "{i:?}"),
            Expr::QualifiedIdentifier(i) => {
                let joined = i
                    .iter()
                    .map(|x| x.value.to_string())
                    .collect::<Vec<String>>()
                    .join(".");

                write!(f, "{joined:?}")
            }
            Expr::Wildcard => write!(f, "*"),
        }
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq, Clone, Copy)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
    And,
    Or,
    Xor,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Plus => f.write_str("+"),
            BinaryOperator::Minus => f.write_str("-"),
            BinaryOperator::Multiply => f.write_str("*"),
            BinaryOperator::Divide => f.write_str("/"),
            BinaryOperator::Modulo => f.write_str("%"),
            BinaryOperator::GreaterThan => f.write_str(">"),
            BinaryOperator::GreaterThanOrEqual => f.write_str(">="),
            BinaryOperator::LessThan => f.write_str("<"),
            BinaryOperator::LessThanOrEqual => f.write_str("<="),
            BinaryOperator::Equal => f.write_str("="),
            BinaryOperator::NotEqual => f.write_str("<>"),
            BinaryOperator::And => f.write_str("AND"),
            BinaryOperator::Or => f.write_str("OR"),
            BinaryOperator::Xor => f.write_str("XOR"),
            BinaryOperator::BitwiseOr => f.write_str("|"),
            BinaryOperator::BitwiseAnd => f.write_str("&"),
            BinaryOperator::BitwiseXor => f.write_str("^"),
        }
    }
}

impl fmt::Debug for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub enum QuoteType {
    None,
    Single,
    Double,
}

#[derive(PartialEq)]
pub enum Value {
    Number(String),
    String(String, QuoteType),
    Boolean(bool),
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Number(n) => f.write_str(n),
            Value::String(s, quote_type) => match quote_type {
                QuoteType::None => f.write_str(s),
                QuoteType::Single => {
                    f.write_str("'")?;
                    f.write_str(s)?;
                    f.write_str("'")
                }
                QuoteType::Double => {
                    f.write_str("\"")?;
                    f.write_str(s)?;
                    f.write_str("\"")
                }
            },
            Value::Boolean(b) => f.write_str(match b {
                true => "TRUE",
                false => "FALSE",
            }),
            Value::Null => f.write_str("NULL"),
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

impl fmt::Debug for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

#[derive(PartialEq)]
pub struct Identifier {
    pub value: String,
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl fmt::Debug for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to fmt::Display
        write!(f, "{}", self)
    }
}

impl Identifier {
    pub fn from(value: String) -> Self {
        Identifier { value }
    }
}
