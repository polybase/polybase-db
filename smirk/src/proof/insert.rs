use std::marker::PhantomData;

use halo2_proofs::{
    plonk,
    poly::commitment::Params,
    transcript::{Blake2bWrite, Challenge255},
};
use rand::{CryptoRng, Rng};

use crate::{
    circuits::insert::circuit::{InsertCircuit, MerklePath},
    Base, Element, Proof, Tree,
};

use super::Insert;
pub fn create<const N: usize>(
    rng: &mut (impl Rng + CryptoRng),
    tree: &Tree<N>,
    element: Element,
) -> Proof<Insert> {
    let params = Params::new(12);
    let (circuit, old_root, new_root) = make_circuit_and_roots(tree, element);
    let (proving_key, verifying_key) = keys(&params, &circuit);

    let mut transcript = Blake2bWrite::<_, _, Challenge255<_>>::init(vec![]);

    plonk::create_proof(
        &params,
        &proving_key,
        &[circuit],
        &[&[&[old_root], &[new_root]]],
        rng,
        &mut transcript,
    )
    .unwrap();

    let transcript_bytes = transcript.finalize();

    Proof {
        transcript_bytes,
        proving_key,
        verifying_key,
        _marker: PhantomData,
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

    let old_root = path.compute_root(Element::NULL.0);
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
