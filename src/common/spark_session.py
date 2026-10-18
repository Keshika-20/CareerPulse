from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional

ROOT = Path(__file__).resolve().parents[2]


def configure_spark_environment() -> None:
    """Make Spark's Python workers use the interpreter running the driver.

    Without this, Spark launches workers with whatever ``python`` is first on
    PATH, which fails with PYTHON_VERSION_MISMATCH when several Pythons are
    installed (common on Windows).
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        # Optional repo-local winutils install, e.g. tools/hadoop/bin/winutils.exe.
        local_hadoop = ROOT / "tools" / "hadoop"
        if (local_hadoop / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(local_hadoop)
            os.environ["PATH"] = f"{local_hadoop / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"


def hadoop_filesystem_available() -> bool:
    """Whether Spark can read and write the local filesystem.

    On Windows, Hadoop's local filesystem needs winutils.exe (HADOOP_HOME). On
    Linux and macOS it always works.
    """
    if os.name != "nt":
        return True
    hadoop_home = os.environ.get("HADOOP_HOME")
    return bool(hadoop_home) and (Path(hadoop_home) / "bin" / "winutils.exe").exists()


def require_hadoop_filesystem(purpose: str) -> None:
    if not hadoop_filesystem_available():
        raise RuntimeError(
            f"{purpose} needs Spark local-filesystem support, which on Windows requires "
            "Hadoop winutils.exe (set HADOOP_HOME, or place it at tools/hadoop/bin/winutils.exe). "
            "Alternatively run it in the Linux Spark container: "
            "docker compose --profile pipeline up -d --build"
        )


def kafka_connector_package() -> str:
    import pyspark

    scala_version = os.environ.get("CAREERPULSE_SCALA_VERSION", "2.13")
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{pyspark.__version__}"


def build_spark_session(
    app_name: str = "CareerPulse",
    master: Optional[str] = None,
    *,
    packages: Iterable[str] = (),
    conf: Optional[Dict[str, str]] = None,
):
    configure_spark_environment()
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Spark features require PySpark and a Java runtime. Install requirements.txt."
        ) from exc

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    builder = (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("CAREERPULSE_SHUFFLE_PARTITIONS", "4"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    package_list = [package for package in packages if package]
    if package_list:
        builder = builder.config("spark.jars.packages", ",".join(package_list))
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("CAREERPULSE_SPARK_LOG_LEVEL", "WARN"))
    return spark
