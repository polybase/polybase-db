/// like smirk but 2
#[macro_export]
macro_rules! smirk2 {
    { $($key:expr => $value:expr $(,)?)* } => {{
        #[allow(clippy::all)]
        let tree: $crate::tree2::Tree2<_, _> = [$(($key, $value)),*].into_iter().collect();
        tree
    }}
}

#[cfg(test)]
mod tests {

    #[test]
    fn simple_syntax_test() {
        let tree = smirk2! {
            1 => "hello",
            2 => "world"  // without trailing comma
        };

        let other_tree = smirk2! {
            1 => "hello",
            2 => "world",  // with trailing comma
        };

        let _many_items = smirk2! {
            1 => "hello",
            2 => "world",
            3 => "foo",
            4 => "bar",
        };
        let _many_items_no_trailing = smirk2! {
            1 => "hello",
            2 => "world",
            3 => "foo",
            4 => "bar"
        };

        let _single_item = smirk2! {
            1 => "hello",
        };

        let _single_item_no_trailing = smirk2! {
            1 => "hello"
        };
    }
}
