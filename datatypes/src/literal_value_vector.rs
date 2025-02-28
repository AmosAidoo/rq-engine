use std::{any::Any, sync::Arc};
use arrow::datatypes::DataType;

use crate::{error::RqEngineError, ColumnVector};

pub struct LiteralValueVector {
  arrow_type: DataType,
  value: Option<Arc<dyn Any>>,
  size: usize
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> &DataType {
      &self.arrow_type
    }

    fn get_value(&self, i: usize) -> Result<Option<Arc<dyn Any>>, RqEngineError> {
      if i > self.size {
        return Err(RqEngineError::IndexOutOfBounds)
      }
      if let Some(value) = self.value.as_ref() {
        Ok(Some(Arc::clone(value)))
      } else {
        // I'm not so sure yet
        Ok(None)
      }
    }

    fn size(&self) -> usize {
      self.size
    }
}