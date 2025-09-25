use crate::series::Series;
use crate::VeloxxError;

impl Series {
    pub fn add(&self, other: &Series) -> Result<Series, VeloxxError> {
        // Check if lengths match
        if self.len() != other.len() {
            return Err(VeloxxError::InvalidOperation(format!(
                "Series length mismatch: {} vs {}",
                self.len(),
                other.len()
            )));
        }

        match (self, other) {
            (Series::I32(name, values, bitmap), Series::I32(_, other_values, other_bitmap)) => {
                let mut new_values = Vec::with_capacity(values.len());
                let mut new_bitmap = Vec::with_capacity(values.len());
                for i in 0..values.len() {
                    if bitmap[i] && other_bitmap[i] {
                        new_values.push(values[i] + other_values[i]);
                        new_bitmap.push(true);
                    } else {
                        new_values.push(0);
                        new_bitmap.push(false);
                    }
                }
                Ok(Series::I32(name.clone(), new_values, new_bitmap))
            }
            (Series::F64(name, values, bitmap), Series::F64(_, other_values, other_bitmap)) => {
                let mut new_values = Vec::with_capacity(values.len());
                let mut new_bitmap = Vec::with_capacity(values.len());
                for i in 0..values.len() {
                    if bitmap[i] && other_bitmap[i] {
                        new_values.push(values[i] + other_values[i]);
                        new_bitmap.push(true);
                    } else {
                        new_values.push(0.0);
                        new_bitmap.push(false);
                    }
                }
                Ok(Series::F64(name.clone(), new_values, new_bitmap))
            }
            // Mixed type arithmetic: F64 + I32 -> F64
            (Series::F64(name, values, bitmap), Series::I32(_, other_values, other_bitmap)) => {
                let mut new_values = Vec::with_capacity(values.len());
                let mut new_bitmap = Vec::with_capacity(values.len());
                for i in 0..values.len() {
                    if bitmap[i] && other_bitmap[i] {
                        new_values.push(values[i] + other_values[i] as f64);
                        new_bitmap.push(true);
                    } else {
                        new_values.push(0.0);
                        new_bitmap.push(false);
                    }
                }
                Ok(Series::F64(name.clone(), new_values, new_bitmap))
            }
            // Mixed type arithmetic: I32 + F64 -> F64
            (Series::I32(name, values, bitmap), Series::F64(_, other_values, other_bitmap)) => {
                let mut new_values = Vec::with_capacity(values.len());
                let mut new_bitmap = Vec::with_capacity(values.len());
                for i in 0..values.len() {
                    if bitmap[i] && other_bitmap[i] {
                        new_values.push(values[i] as f64 + other_values[i]);
                        new_bitmap.push(true);
                    } else {
                        new_values.push(0.0);
                        new_bitmap.push(false);
                    }
                }
                Ok(Series::F64(name.clone(), new_values, new_bitmap))
            }
            _ => Err(VeloxxError::InvalidOperation(
                "Addition not supported for these series types".to_string(),
            )),
        }
    }

    pub fn multiply(&self, other: &Series) -> Result<Series, VeloxxError> {
        match (self, other) {
            (Series::I32(name, values, bitmap), Series::I32(_, other_values, other_bitmap)) => {
                let mut new_values = Vec::with_capacity(values.len());
                let mut new_bitmap = Vec::with_capacity(values.len());
                for i in 0..values.len() {
                    if bitmap[i] && other_bitmap[i] {
                        new_values.push(values[i] * other_values[i]);
                        new_bitmap.push(true);
                    } else {
                        new_values.push(0);
                        new_bitmap.push(false);
                    }
                }
                Ok(Series::I32(name.clone(), new_values, new_bitmap))
            }
            (Series::F64(name, values, bitmap), Series::F64(_, other_values, other_bitmap)) => {
                let mut new_values = Vec::with_capacity(values.len());
                let mut new_bitmap = Vec::with_capacity(values.len());
                for i in 0..values.len() {
                    if bitmap[i] && other_bitmap[i] {
                        new_values.push(values[i] * other_values[i]);
                        new_bitmap.push(true);
                    } else {
                        new_values.push(0.0);
                        new_bitmap.push(false);
                    }
                }
                Ok(Series::F64(name.clone(), new_values, new_bitmap))
            }
            _ => Err(VeloxxError::InvalidOperation(
                "Multiplication not supported for these series types".to_string(),
            )),
        }
    }
}
