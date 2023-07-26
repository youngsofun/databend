use std::collections::BTreeMap;

use arrow_schema::Schema;

/// Project a [`Schema`] by picking the fields at the given indices.
pub fn project(schema: &Schema, indices: &[usize]) -> Schema {
    let fields = indices
        .iter()
        .map(|idx| schema.fields[*idx].clone())
        .collect::<Vec<_>>();
    Schema::with_metadata(fields.into(), schema.metadata.clone())
}

/// Project a [`Schema`] with inner columns by path.
pub fn inner_project(schema: &Schema, path_indices: &BTreeMap<usize, Vec<usize>>) -> Schema {
    let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
    let fields = paths
        .iter()
        .map(|path| traverse_paths(&schema.fields, path))
        .collect::<Vec<_>>();
    Schema::with_metadata(fields.into(), schema.metadata.clone())
}
