use std::{borrow::Cow, rc::Rc, sync::Arc};

use miden_crypto::hash::rpo::Rpo256;

use super::Digest;

/// Types which can be hashed
///
/// This trait is primarily used as a bound on the value type for most useful functions on
/// [`MerkleTree`]
///
/// [`MerkleTree`]: crate::MerkleTree
pub trait Hashable {
    /// Compute the hash of this object
    ///
    /// ```rust
    /// # use smirk::hash::Hashable;
    /// let digest = 1i32.hash();
    /// println!("the hash of 1 is {digest}");
    /// ```
    fn hash(&self) -> Digest;
}

impl<T: ?Sized> Hashable for &T
where
    T: Hashable,
{
    fn hash(&self) -> Digest {
        <T as Hashable>::hash(self)
    }
}

impl<T: ?Sized> Hashable for &mut T
where
    T: Hashable,
{
    fn hash(&self) -> Digest {
        <T as Hashable>::hash(self)
    }
}

impl<T: ?Sized> Hashable for Box<T>
where
    T: Hashable,
{
    fn hash(&self) -> Digest {
        <T as Hashable>::hash(self)
    }
}

impl<'a, T: ?Sized> Hashable for Cow<'a, T>
where
    T: Hashable + Clone,
{
    fn hash(&self) -> Digest {
        <T as Hashable>::hash(self)
    }
}

impl<T: ?Sized> Hashable for Rc<T>
where
    T: Hashable,
{
    fn hash(&self) -> Digest {
        <T as Hashable>::hash(self)
    }
}

impl<T: ?Sized> Hashable for Arc<T>
where
    T: Hashable,
{
    fn hash(&self) -> Digest {
        <T as Hashable>::hash(self)
    }
}

macro_rules! int_impl {
    ($int:ty) => {
        impl Hashable for $int {
            fn hash(&self) -> Digest {
                Digest(Rpo256::hash(&self.to_be_bytes()))
            }
        }
    };
}

int_impl!(i8);
int_impl!(i16);
int_impl!(i32);
int_impl!(i64);
int_impl!(i128);
int_impl!(isize);
int_impl!(u8);
int_impl!(u16);
int_impl!(u32);
int_impl!(u64);
int_impl!(u128);
int_impl!(usize);

/// impl for any type that implements `AsRef<[u8]>`
macro_rules! as_ref_impl {
    ($t:ty) => {
        impl Hashable for $t {
            fn hash(&self) -> Digest {
                let bytes = <$t as AsRef<[u8]>>::as_ref(self);
                Digest(Rpo256::hash(bytes))
            }
        }
    };
}

impl<const N: usize> Hashable for [u8; N] {
    fn hash(&self) -> Digest {
        let bytes = <[u8; N] as AsRef<[u8]>>::as_ref(self);
        Digest(Rpo256::hash(bytes))
    }
}

// note, we implement the trait on `[u8]`, not `&[u8]` so it works with the above impls for types
// like `Arc<[u8]>` or `Box<[u8]>` - the same logic applies to `str`
as_ref_impl!([u8]);
as_ref_impl!(Vec<u8>);
as_ref_impl!(str);
as_ref_impl!(String);
