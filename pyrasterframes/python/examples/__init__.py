#examples_setup
from pathlib import Path
import os
jarpath = list(Path('../target').resolve().glob('**/pyrasterframes*.jar'))
if len(jarpath) > 0:
    os.environ["SPARK_CLASSPATH"] = jarpath[0].as_uri()
# hard-coded relative path for resources
resource_dir = Path('./static').resolve()
#examples_setup
