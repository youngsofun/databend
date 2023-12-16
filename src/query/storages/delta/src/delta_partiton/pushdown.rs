use common_catalog::plan::{Projection, PushDownInfo};
use common_expression::FieldIndex;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::date_helper::today_date;

fn get_pushdown_without_partition_columns(
    mut pushdown: PushDownInfo,
    mut partition_columns: Vec<FieldIndex>,
    num_columns: usize,
) -> Result<PushDownInfo> {
    if partition_columns.is_empty() {
        return Ok(pushdown);
    }
    todo!()
}

fn shift_projection_index(field_index: FieldIndex, partition_columns: &[FieldIndex]) -> Option<Result<FieldIndex>> {
    let mut sub = 0;
    // most of the time, there a few partition columns, so let`s keep it simple.
    for col in partition_columns {
        if field_index == *col {
            return None;
        }
        if field_index > *col {
            sub += 1;
        }
    }
    if field_index < sub {
        Some(Err(ErrorCode::BadArguments(format!(
            "bug: field_index: {}, partition_columns: {:?}",
            field_index, partition_columns
        ))))
    } else {
        Some(Ok(field_index - sub))
    }
}

fn shift_projection(mut prj: Projection, partition_columns: &[FieldIndex]) {
    let f = |prc| {
        shift_projection_index(prc, partition_columns).unwrap()
    };
    match prj {
        Projection::InnerColumns(mut map) => {
            map = map.iter().filter_map(|(i, columns)| shift_projection_index(*i, partition_columns)).collect()?
        }
        Projection::Columns(mut columns) => {
            todo!()
        }
    }
    todo!()
}

#[cfg(test)]
mod tests {
    use crate::delta_partiton::pushdown::shift_projection_index;

    #[test]
    fn test_shift_projection_index() {
        assert_eq!(shift_projection_index(1,&[0, 2]), Some(Ok(0)));
    }
}