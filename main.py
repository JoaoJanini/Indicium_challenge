
import luigi
# from stepTwo import ExtractLocal hello
from stepTwo import ExtractLocal

if __name__ == "__main__":
    # Use local scheduler for development purposes.
    luigi.run(["--local-scheduler"], main_task_cls= ExtractLocal)