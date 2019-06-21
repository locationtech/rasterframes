# examples_setup
import os

__all__ = ["test_resource_dir"]


def test_resource_dir():
    here = os.path.dirname(os.path.realpath(__file__))
    scala_target = os.path.realpath(os.path.join(here, '..', '..', 'scala-2.11'))
    return os.path.realpath(os.path.join(scala_target, 'test-classes'))
