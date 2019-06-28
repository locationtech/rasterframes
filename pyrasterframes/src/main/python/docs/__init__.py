import os


# This is temporary until we port to run on web assets.
def resource_dir():
    # we may consider using gitpython which I think would be appropriate in the context of building docs
    # see https://stackoverflow.com/a/41920796/2787937
    here = os.path.dirname(os.path.realpath(__file__))
    test_resource = os.path.realpath(os.path.join(here, '..', '..', '..', 'src', 'test', 'resources'))

    return test_resource


def resource_dir_uri():
    return 'file://' + resource_dir()
