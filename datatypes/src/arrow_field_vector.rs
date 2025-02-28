use std::{any::Any, sync::Arc};
use arrow::{array::{Array, BooleanArray, BooleanBuilder, Int8Builder}, datatypes::DataType, error::ArrowError};

use crate::{error::RqEngineError, ColumnVector};

/// FieldVector provides columnar storage for data for a field
type FieldVector = Arc<dyn Array>;

struct FieldVectorFactory;

impl FieldVectorFactory {
  fn create(data_type: &DataType, initial_capacity: usize) -> Result<FieldVector, ArrowError> {
    match data_type {
      DataType::Boolean => {
        let mut builder = BooleanBuilder::with_capacity(initial_capacity);
        Ok(Arc::new(builder.finish()))
      },
      DataType::Int8 => {
        let mut builder = Int8Builder::with_capacity(initial_capacity);
        Ok(Arc::new(builder.finish()))
      },
      _ => {
        Err(ArrowError::NotYetImplemented(format!("DataType {:?} is not supported", data_type)))
      }
    }
  }
}

pub struct ArrowFieldVector {
 field: FieldVector
}

impl ColumnVector for ArrowFieldVector {
    fn get_type(&self) -> &arrow::datatypes::DataType {
      self.field.data_type()
    }

    fn get_value(&self, i: usize) -> Result<Option<Arc<dyn Any>>, RqEngineError> {
      if self.field.is_null(i) {
        return Ok(None)
      } else {
        match self.get_type() {
          DataType::Boolean => {
            let boolean_array = self.field.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Some(Arc::new(boolean_array.value(i))))
          },
          _ => {
            Err(RqEngineError::NotImplemented(format!("DataType {:?} is not supported", self.get_type())))
          }
        }
      }
    }

    fn size(&self) -> usize {
      self.field.len()
    }
}