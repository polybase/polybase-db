use core::fmt::Display;

use crate::Base;
use ethnum::U256;
use halo2_proofs::{arithmetic::Field, pasta::group::ff::PrimeFieldBits};

mod lsb;
pub use lsb::Lsb;

#[cfg(feature = "serde")]
mod serde;

/// A 256-bit field element
///
///
///
/// Internally, this is represented by a base field element of the [pallas][pallas] curve:
/// [`Base`].
///
/// [pallas]: https://electriccoin.co/blog/the-pasta-curves-for-halo-2-and-beyond/
/// [`Base`]: halo2_proofs::pasta::pallas::Base
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Element(pub(crate) Base);

impl core::hash::Hash for Element {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.0.to_le_bits().hash(state);
    }
}

impl Display for Element {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let u256 = U256::from_le_bytes(self.0.into());
        write!(f, "{u256}")
    }
}

impl Element {
    /// The zero element of the group (the additive identity)
    pub const ZERO: Self = Self(Base::ZERO);

    /// The one element of the group (the multiplicative identity)
    pub const ONE: Self = Self(Base::ONE);

    /// A null hash value (this is identical to [`Element::ZERO`])
    ///
    /// Note that this value is chosen arbitrarily
    pub const NULL_HASH: Self = Self::ZERO;

    /// Create a new [`Element`] from a u64
    #[inline]
    #[must_use]
    pub fn from_u64(i: u64) -> Self {
        Self(Base::from(i))
    }

    /// Create an [`Element`] from the [`Base`] element that represents it
    ///
    /// [`Base`]: halo2_proofs::pasta::pallas::Base
    #[inline]
    #[must_use]
    pub fn from_base(base: Base) -> Self {
        Self(base)
    }

    /// Get the underlying [`Base`] that this [`Element`] represents
    ///
    /// [`Base`]: halo2_proofs::pasta::pallas::Base
    #[inline]
    #[must_use]
    pub fn into_base(self) -> Base {
        self.0
    }
}

impl From<Base> for Element {
    fn from(value: Base) -> Self {
        Element(value)
    }
}

impl From<Element> for Base {
    fn from(value: Element) -> Self {
        value.0
    }
}

macro_rules! arith_op {
    ($trait:ident, $f:ident, $($t:tt)*) => {
        impl core::ops::$trait<Element> for Element {
            type Output = Element;

            fn $f(self, rhs: Element) -> Self::Output {
                Element(self.0 $($t)* rhs.0)
            }
        }

        impl core::ops::$trait<u64> for Element {
            type Output = Element;

            fn $f(self, rhs: u64) -> Self::Output {
                self $($t)* Element::from_u64(rhs)
            }
        }


    };
}

arith_op!(Add, add, +);
arith_op!(Sub, sub, -);
arith_op!(Mul, mul, *);

impl PartialEq<u64> for Element {
    fn eq(&self, other: &u64) -> bool {
        *self == Element::from_u64(*other)
    }
}

impl core::ops::Neg for Element {
    type Output = Self;
    fn neg(self) -> Self::Output {
        Element(-self.0)
    }
}

impl core::iter::Sum<Element> for Element {
    fn sum<I: Iterator<Item = Element>>(iter: I) -> Self {
        iter.fold(Element::ZERO, |a, b| a + b)
    }
}

impl core::iter::Product<Element> for Element {
    fn product<I: Iterator<Item = Element>>(iter: I) -> Self {
        iter.fold(Element::ONE, |a, b| a * b)
    }
}

impl From<u64> for Element {
    fn from(value: u64) -> Self {
        Self::from_u64(value)
    }
}

#[cfg(any(test, feature = "proptest"))]
mod proptest {
    use super::{Base, Element};
    use ::proptest::{arbitrary::StrategyFor, prelude::*, strategy::Map};
    use halo2_proofs::pasta::group::ff::FromUniformBytes;

    impl Arbitrary for Element {
        type Strategy = Map<StrategyFor<[u8; 64]>, fn([u8; 64]) -> Self>;
        type Parameters = ();

        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            any::<[u8; 64]>().prop_map(|array| {
                let element = Self(Base::from_uniform_bytes(&array));

                if element == Element::NULL_HASH {
                    Element::ONE
                } else {
                    element
                }
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::Element;

    #[test]
    fn syntax_test() {
        let element = Element::from_u64(123);

        assert_eq!(element + 1, Element::from_u64(124));
        assert_eq!(element * 2, Element::from_u64(246));
        assert_eq!(element - 2, Element::from_u64(121));
        assert_eq!(element + Element::ONE, Element::from_u64(124));
        assert_eq!(element * Element::from_u64(2), Element::from_u64(246));
        assert_eq!(element - Element::from_u64(2), Element::from_u64(121));

        println!("{element:?} {element}");

        assert_eq!(Element::from(1).to_string(), "1");
        assert_eq!(Element::from(100).to_string(), "100");
        assert_eq!(Element::from(123).to_string(), "123");

        assert_eq!(
            (1..=10).map(Element::from_u64).sum::<Element>(),
            Element::from(55)
        );

        assert_eq!(
            (1..=5).map(Element::from_u64).product::<Element>(),
            Element::from(120)
        );
    }
}
