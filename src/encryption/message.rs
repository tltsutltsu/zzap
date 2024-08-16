use super::EncryptionError;

pub trait Message {
    fn as_boolean(&self) -> Result<Vec<bool>, EncryptionError>;
    fn from_boolean(&self, boolean: Vec<bool>) -> Result<String, EncryptionError>;
}

impl Message for String {
    fn as_boolean(&self) -> Result<Vec<bool>, EncryptionError> {
        // convert string to binary representation
        // like 0b01010101
        // 0b01010101 -> [false, true, false, true, false, true, false, true]

        let mut binary = Vec::new();

        for c in self.chars() {
            let byte = c as u8;
            for i in 0..8 {
                binary.push((byte >> i) & 1 == 1);
            }
        }

        Ok(binary)
    }

    fn from_boolean(&self, boolean: Vec<bool>) -> Result<String, EncryptionError> {
        let mut string = String::new();

        for chunk in boolean.chunks(8) {
            let mut byte = 0;
            for (i, &bit) in chunk.iter().enumerate() {
                if bit {
                    byte |= 1 << i;
                }
            }
            string.push(char::from(byte));
        }

        Ok(string)
    }
}