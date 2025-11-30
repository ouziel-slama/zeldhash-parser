pub(super) trait Overlay: Sized {
    fn overlay(self, overrides: Self) -> Self;
}
