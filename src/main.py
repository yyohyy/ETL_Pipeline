import glob
import importlib
import sys
from library.logger import Logger

logger = Logger("ETL")

def execute_etl_scripts(src_dir="src"):
    try:
        sys.path.append(src_dir)
        script_files = glob.glob(f"{src_dir}/*-ext.py")
        tables = [file.split("\\")[-1][:-7] for file in script_files]

        for table in tables:
            module_name = f"{table}-ext"
            function_name = f"{table}_etl"

            try:
                module = importlib.import_module(module_name)
                if hasattr(module, function_name):
                    logger.log_info(f"Executing {function_name}() from {module_name}.py")
                    getattr(module, function_name)()
                else:
                    logger.log_info(f"Skipping {module_name}.py: No function '{function_name}()' found.")
            except Exception as e:
                logger.log_error(f"Error executing {module_name}.py: {e}")
    except Exception as e:
        logger.log_error(f"Error during ETL execution: {e}")

if __name__ == "__main__":
    execute_etl_scripts()
