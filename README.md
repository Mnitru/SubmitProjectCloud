# Group 5 members:
1. Le Quoc Trung
2. Nguyen Duy Anh Tung
3. Nguyen Xuan Vinh - 2540027
4. Trinh Thanh Vinh - 2540110
# Data compression in Spark and HDFS – Group 5

InfraCloud project, USTH. Storage-level (HDFS/Parquet) and communication-level (Spark shuffle)
compression with LZ4 and zstd, measured on a Spark/HDFS cluster of 4 VMs on two physical machines.

| | |
|---|---|
| Report | [`report/report.pdf`](report/report.pdf) |
| Slides | [`slides/compression_spark_hdfs.pptx`](slides/compression_spark_hdfs.pptx) (speaker notes included) |
| Results | [`results/`](results/): `runs.jsonl` (one record per run, 351 runs), `raw/` (Spark event log and 1 s node metrics of every run), `figures/`, `tables/` |
| Code | `cluster.conf`, `infra/` (cluster), `apps/` (Spark jobs), `experiments/` (runner), `codec-bench/`, `analysis/` (figures and statistics) |

**Main result (4 GB):** zstd at both levels makes the I/O-bound Sort 2.6× faster and the balanced Join
1.7× faster; the CPU-bound Hash is unchanged. No configuration was measurably slower than no
compression. Without the 100 Mbit/s network cap the gain disappears: the benefit comes from the network
bottleneck.

## Recompute the figures and tables from the results

Python 3.9 or newer:

```bash
pip install -r requirements.txt
python analysis/make_plots.py        # figures 01-12, 14 and the summary tables
python analysis/extra_analysis.py    # significance tests, cost model, control experiments (figures 13, 15)
python analysis/summary_figures.py   # figures 16, 17
```

Output in `results/figures/` and `results/tables/`.

## Rebuild the cluster and measure again

Needs a Windows Pro PC with Hyper-V (master, worker1, worker2) and a second Windows PC with VirtualBox
(worker3) on the same home network, both with [Multipass](https://multipass.run). Adapt the LAN
addresses and the Hyper-V switch name in `cluster.conf` if needed. Commands run from this folder.

1. Desktop, create the 3 VMs and the WireGuard network:
   `powershell -ExecutionPolicy Bypass -File infra\windows\desktop.ps1 up`
2. Laptop (administrator PowerShell), create worker3 with the two commands printed by step 1
   (`Invoke-WebRequest … worker3.yaml` then `multipass launch … --name worker3 …`).
3. Desktop, install and start HDFS, Spark and monitoring; the health check must end with `10 OK, 0 FAIL`:
   `powershell -ExecutionPolicy Bypass -File infra\windows\desktop.ps1 install`
4. Open a shell on master (`desktop.ps1 shell`) and run the phases one after the other in the
   background (each is resumable; follow them with `tail -f ~/study/results/campaign.log`):

   ```bash
   cd ~/study/experiments
   sudo systemd-run --unit=campaign-prep --uid=ubuntu --setenv=HOME=/home/ubuntu bash /home/ubuntu/study/experiments/campaign.sh prep "0.5 1 2 4"   # datasets + codec benchmark, ~45 min
   sudo systemd-run --unit=campaign-run --uid=ubuntu --setenv=HOME=/home/ubuntu bash /home/ubuntu/study/experiments/campaign.sh run "0.5 1 2 4" 3   # 216 runs, ~10 h
   sudo systemd-run --unit=extra-run --uid=ubuntu --setenv=HOME=/home/ubuntu bash /home/ubuntu/study/experiments/extra.sh                          # with / without the cap, ~1.5 h
   sudo systemd-run --unit=random-run --uid=ubuntu --setenv=HOME=/home/ubuntu bash /home/ubuntu/study/experiments/extra_random.sh                 # incompressible data, ~35 min
   ```

5. Desktop, copy the results back, then recompute the figures as above:
   `powershell -ExecutionPolicy Bypass -File infra\windows\desktop.ps1 pull`

Other `desktop.ps1` commands: `start` and `stop` (after or before a reboot; start worker3 on the laptop with
`multipass start worker3`), `status` (health check), `push` (copy the code to master).
Web interfaces on master's LAN address: Spark `:8080`, HDFS `:9870`, Grafana `:3000`, Prometheus `:9090`.
