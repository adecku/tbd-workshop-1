from pathlib import Path
import json
import shutil
import subprocess
import time


def find_gcloud():
    gcloud = shutil.which("gcloud.cmd") or shutil.which("gcloud")

    if gcloud is not None:
        return gcloud

    possible_paths = [
        r"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        r"C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        str(Path.home() / r"AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"),
    ]

    for path in possible_paths:
        if Path(path).exists():
            return path

    raise RuntimeError(
        "Nie znaleziono gcloud/gcloud.cmd. "
        "Uruchom notebook z terminala, w którym działa `gcloud`, albo dodaj Google Cloud SDK do PATH."
    )


GCLOUD = find_gcloud()


def fix_cmd(cmd):
    cmd = [str(x) for x in cmd]
    if cmd[0] == "gcloud":
        cmd[0] = GCLOUD
    return cmd


def printable_cmd(cmd):
    return " ".join(str(x) for x in cmd)


def run_cmd(cmd, check=True):
    cmd = fix_cmd(cmd)
    print("+", printable_cmd(cmd))
    return subprocess.run(cmd, check=check, text=True)


def capture_cmd(cmd):
    cmd = fix_cmd(cmd)
    print("+", printable_cmd(cmd))
    return subprocess.check_output(cmd, text=True).strip()


def check_dataproc_cluster(project_id, region, cluster_name):
    clusters = capture_cmd([
        "gcloud", "dataproc", "clusters", "list",
        "--project", project_id,
        "--region", region,
        "--format", "value(clusterName)",
    ])

    available_clusters = [c.strip() for c in clusters.splitlines() if c.strip()]

    if cluster_name not in available_clusters:
        raise RuntimeError(
            f"Cluster {cluster_name!r} was not found in region {region}. "
            f"Available clusters: {available_clusters}. "
            "If the list is empty, run terraform apply first or check whether resources were destroyed."
        )

    return available_clusters


def upload_path_to_gcs(local_path, gcs_path):
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"Missing local path: {local_path}")

    if local_path.is_dir():
        run_cmd([
            "gcloud", "storage", "rsync", "-r",
            str(local_path),
            gcs_path,
        ])
    else:
        run_cmd([
            "gcloud", "storage", "cp",
            str(local_path),
            gcs_path,
        ])


def upload_task5_inputs(
    events_path,
    dimension_path,
    partitioned_events_dir,
    gcs_events_path,
    gcs_dimension_path,
    gcs_partitioned_events_dir,
):
    upload_path_to_gcs(events_path, gcs_events_path)
    upload_path_to_gcs(dimension_path, gcs_dimension_path)

    partitioned_events_dir = Path(partitioned_events_dir)

    if partitioned_events_dir.exists():
        upload_path_to_gcs(partitioned_events_dir, gcs_partitioned_events_dir)
        return gcs_partitioned_events_dir, "gcs_partitioned_by_event_date"

    return gcs_events_path, "gcs_default"


def upload_job_script(local_job_script_path, gcs_job_script):
    upload_path_to_gcs(local_job_script_path, gcs_job_script)


def submit_dataproc_job(
    project_id,
    region,
    cluster_name,
    gcs_job_script,
    gcs_events_path,
    gcs_partitioned_or_default_path,
    gcs_dimension_path,
    gcs_results_dir,
    rows,
    events_size_mb,
    partitioned_size_mb,
    repeats=3,
):
    start = time.perf_counter()

    run_cmd([
        "gcloud", "dataproc", "jobs", "submit", "pyspark", gcs_job_script,
        "--project", project_id,
        "--region", region,
        "--cluster", cluster_name,
        "--properties", "spark.sql.shuffle.partitions=8",
        "--",
        "--events-path", gcs_events_path,
        "--partitioned-events-path", gcs_partitioned_or_default_path,
        "--dimension-path", gcs_dimension_path,
        "--output-path", gcs_results_dir,
        "--rows", str(rows),
        "--events-size-mb", str(events_size_mb),
        "--partitioned-size-mb", str(partitioned_size_mb),
        "--repeats", str(repeats),
    ])

    return round(time.perf_counter() - start, 4)


def download_dataproc_results(gcs_results_dir, local_results_dir):
    local_results_dir = Path(local_results_dir)

    if local_results_dir.exists():
        shutil.rmtree(local_results_dir)

    local_results_dir.mkdir(parents=True, exist_ok=True)

    part_files_raw = capture_cmd([
        "gcloud", "storage", "ls",
        f"{gcs_results_dir}/part-*",
    ])

    part_files = [p.strip() for p in part_files_raw.splitlines() if p.strip()]

    for part_file in part_files:
        run_cmd([
            "gcloud", "storage", "cp",
            part_file,
            str(local_results_dir),
        ])

    rows = []

    for file in local_results_dir.glob("part-*"):
        with file.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))

    return rows