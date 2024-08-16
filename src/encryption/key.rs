use concrete_csprng::generators::SoftwareRandomGenerator;
use tfhe::boolean::{client_key::ClientKey, parameters::PARAMETERS_ERROR_PROB_2_POW_MINUS_165};
use tfhe::core_crypto::commons::generators::SecretRandomGenerator;
use tfhe::core_crypto::prelude::{allocate_and_generate_new_binary_lwe_secret_key, allocate_and_generate_new_binary_glwe_secret_key};
use tfhe::Seed;

use super::EncryptionError;

pub trait Key {
    fn to_tfhe(&self) -> Result<ClientKey, EncryptionError>;
}

impl Key for String {
    fn to_tfhe(&self) -> Result<ClientKey, EncryptionError> {
        let seed = self.as_bytes()[0..16].try_into().map_err(|_| EncryptionError::WrongKeySize)?;
        let seed = Seed(u128::from_le_bytes(seed));

        let parameters = PARAMETERS_ERROR_PROB_2_POW_MINUS_165;

        let mut generator: SecretRandomGenerator<SoftwareRandomGenerator> = SecretRandomGenerator::new(seed);

        let lwe_secret_key = allocate_and_generate_new_binary_lwe_secret_key(
            parameters.lwe_dimension,
            &mut generator,
        );

        // generate the glwe secret key
        let glwe_secret_key = allocate_and_generate_new_binary_glwe_secret_key(
            parameters.glwe_dimension,
            parameters.polynomial_size,
            &mut generator,
        );

        Ok(ClientKey::new_from_raw_parts(lwe_secret_key, glwe_secret_key, PARAMETERS_ERROR_PROB_2_POW_MINUS_165))
    }
}