use crate::{ColumnVector, Schema};

struct RecordBatch {
  schema: Schema,
  fields: Vec<Box<dyn ColumnVector>>
}

impl RecordBatch {
  fn row_count(&self) -> usize {
    if let Some(first) = self.fields.first() {
      first.as_ref().size()
    } else {
      0
    }
  }

  fn column_count(&self) -> usize {
    self.fields.len()
  }

  fn field(&self, i: usize) -> &Box<dyn ColumnVector> {
    &self.fields[i]
  }
}