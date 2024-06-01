// pub struct Arena {
//     nodes: Vec<Node>,
// }

pub enum Program {
    Select,
    Update,
    Insert,
    Delete,
}

// impl fmt::Debug for ColumnDefinitionCollection {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         Ok(())
//     }
// }

// impl Arena {
//     pub fn new() -> Arena {
//         Arena { nodes: vec![] }
//     }

//     pub fn add_node(&mut self, node: Program) -> usize {
//         let index = self.nodes.len();

//         self.nodes.push(node);

//         index
//     }

//     pub fn collect(&self) -> &Vec<Program> {
//         &self.nodes
//     }
// }

// #[derive(Debug)]
// pub struct Node {
//     children: Vec<Node>,
// }

// impl Node {
//     pub fn new() -> Node {
//         Node { children: vec![] }
//     }

//     pub fn add_node(&mut self, node: Node) {
//         self.children.push(node);
//     }
// }
