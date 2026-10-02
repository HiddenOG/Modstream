"""CyberShield: real-time harmful content detection for message streams."""

__version__ = "3.0.0"


def create_app(*args, **kwargs):
    from .main import create_app as _create_app

    return _create_app(*args, **kwargs)
