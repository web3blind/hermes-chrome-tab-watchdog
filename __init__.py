"""Hermes directory-plugin entrypoint; implementation lives in one package."""
def register(ctx):
    from .tabwatch_plugin import register as implementation
    return implementation(ctx)

__all__ = ['register']
