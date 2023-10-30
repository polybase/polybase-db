use halo2_proofs::{
    pasta::EqAffine,
    plonk::{self, keygen_pk, keygen_vk, ProvingKey, VerifyingKey},
    poly::commitment::Params,
    transcript::{Blake2bWrite, Challenge255},
};
use rand::{CryptoRng, Rng};

use crate::{circuits::insert::circuit::InsertCircuit, Element, Tree};

use super::{circuit::MerklePath, Base};

type Affine = halo2_proofs::pasta::vesta::Affine;

/// A proof of a correct insert
pub struct Proof {
    transcript_bytes: Vec<u8>,
    pk: ProvingKey<EqAffine>,
    vk: VerifyingKey<EqAffine>,
}

pub fn create<const N: usize>(
    rng: &mut (impl Rng + CryptoRng),
    tree: &Tree<N>,
    element: Element,
) -> Proof {
    let params = Params::new(12);
    let (circuit, old_root, new_root) = make_circuit_and_roots(tree, element);
    let (pk, vk) = keys(&params, &circuit);

    let mut transcript = Blake2bWrite::<_, _, Challenge255<_>>::init(vec![]);

    plonk::create_proof(
        &params,
        &pk,
        &[circuit],
        &[&[&[old_root], &[new_root]]],
        rng,
        &mut transcript,
    )
    .unwrap();

    let transcript_bytes = transcript.finalize();

    Proof {
        transcript_bytes,
        pk,
        vk,
    }
}

fn make_circuit_and_roots<const N: usize>(
    tree: &Tree<N>,
    element: Element,
) -> (InsertCircuit<N>, Base, Base) {
    let path = tree.path_for(element);
    let path = MerklePath {
        siblings: path.siblings.into_iter().map(|Element(e)| e).collect(),
    };

    let circuit = InsertCircuit::new(element.0, path.clone());

    let old_root = path.compute_root(Element::NULL_HASH.0);
    let new_root = path.compute_root(element.0);

    (circuit, old_root, new_root)
}

fn keys<const N: usize>(
    params: &Params<Affine>,
    circuit: &InsertCircuit<N>,
) -> (ProvingKey<Affine>, VerifyingKey<Affine>) {
    let vk = keygen_vk(params, circuit).unwrap();
    let pk = keygen_pk(params, vk.clone(), circuit).unwrap();

    (pk, vk)
}
