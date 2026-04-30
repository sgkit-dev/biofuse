"""Declarative list of plink operations exercised by the IO-pattern study.

Each ``Operation`` describes one plink invocation. The harness in ``run.py``
fills in ``--bfile`` and ``--out`` and provides any auxiliary files
referenced by name in ``aux``.

``aux`` keys map to generators known to ``run.py``. Each generator emits a
file in the per-op aux directory; the path is then substituted into the
``argv`` template via ``${name}``.

``argv`` substitution syntax: ``${aux:name}`` substitutes the file path
of an aux file produced by generator ``name``.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    id: str
    tool: str  # "plink1.9" or "plink2"
    category: str
    label: str
    argv: tuple[str, ...]
    aux: tuple[str, ...] = ()
    expensive: bool = False


# ---------------------------------------------------------------------------
# Auxiliary file generators (referenced by name in operations).
# These names are recognised by ``run.py``:
#
#   extract_10           — 10 variant IDs
#   extract_1k           — 1,000 variant IDs
#   extract_100k         — 100,000 variant IDs
#   keep_10              — 10 samples (FID + IID)
#   pheno                — random quantitative phenotype, all samples
#
# To request a file, list its name in ``aux`` and reference it in ``argv``
# as ``${aux:NAME}``.
# ---------------------------------------------------------------------------


OPERATIONS: tuple[Operation, ...] = (
    # -----------------------------------------------------------------
    # Whole-genome scans (no input restriction)
    # -----------------------------------------------------------------
    Operation("p19_freq", "plink1.9", "scan", "--freq (full scan)", ("--freq",)),
    Operation("p19_missing", "plink1.9", "scan", "--missing", ("--missing",)),
    Operation("p19_hardy", "plink1.9", "scan", "--hardy", ("--hardy",)),
    Operation("p19_het", "plink1.9", "scan", "--het", ("--het",)),
    Operation("p2_freq", "plink2", "scan", "--freq (full scan)", ("--freq",)),
    Operation("p2_missing", "plink2", "scan", "--missing", ("--missing",)),
    Operation("p2_hardy", "plink2", "scan", "--hardy", ("--hardy",)),
    Operation("p2_het", "plink2", "scan", "--het", ("--het",)),
    # -----------------------------------------------------------------
    # Output operations (read + write)
    # -----------------------------------------------------------------
    Operation("p19_make_bed", "plink1.9", "output", "--make-bed", ("--make-bed",)),
    Operation("p2_make_bed", "plink2", "output", "--make-bed", ("--make-bed",)),
    Operation("p19_recode_A", "plink1.9", "output", "--recode A", ("--recode", "A")),
    Operation("p2_export_A", "plink2", "output", "--export A", ("--export", "A")),
    # -----------------------------------------------------------------
    # Range filters: chromosome
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_chr1",
        "plink1.9",
        "range_filter",
        "--freq --chr 1",
        ("--freq", "--chr", "1"),
    ),
    Operation(
        "p19_freq_chr1_3",
        "plink1.9",
        "range_filter",
        "--freq --chr 1-3",
        ("--freq", "--chr", "1-3"),
    ),
    Operation(
        "p2_freq_chr1",
        "plink2",
        "range_filter",
        "--freq --chr 1",
        ("--freq", "--chr", "1"),
    ),
    Operation(
        "p2_freq_chr1_3",
        "plink2",
        "range_filter",
        "--freq --chr 1-3",
        ("--freq", "--chr", "1-3"),
    ),
    # -----------------------------------------------------------------
    # Range filters: bp position
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_bp_window_chr1",
        "plink1.9",
        "range_filter",
        "--freq --chr 1 --from-bp 0 --to-bp 100000",
        ("--freq", "--chr", "1", "--from-bp", "0", "--to-bp", "100000"),
    ),
    Operation(
        "p2_freq_bp_window_chr1",
        "plink2",
        "range_filter",
        "--freq --chr 1 --from-bp 0 --to-bp 100000",
        ("--freq", "--chr", "1", "--from-bp", "0", "--to-bp", "100000"),
    ),
    # -----------------------------------------------------------------
    # Range filters: SNP-id range
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_snp_range",
        "plink1.9",
        "range_filter",
        "--freq --snp rs500000 --window 1000",
        ("--freq", "--snp", "rs500000", "--window", "1000"),
    ),
    # -----------------------------------------------------------------
    # SNP-list filters
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_extract_10",
        "plink1.9",
        "list_filter",
        "--freq --extract <10 SNPs>",
        ("--freq", "--extract", "${aux:extract_10}"),
        aux=("extract_10",),
    ),
    Operation(
        "p19_freq_extract_1k",
        "plink1.9",
        "list_filter",
        "--freq --extract <1k SNPs>",
        ("--freq", "--extract", "${aux:extract_1k}"),
        aux=("extract_1k",),
    ),
    Operation(
        "p19_freq_extract_100k",
        "plink1.9",
        "list_filter",
        "--freq --extract <100k SNPs>",
        ("--freq", "--extract", "${aux:extract_100k}"),
        aux=("extract_100k",),
    ),
    Operation(
        "p19_freq_exclude_10",
        "plink1.9",
        "list_filter",
        "--freq --exclude <10 SNPs>",
        ("--freq", "--exclude", "${aux:extract_10}"),
        aux=("extract_10",),
    ),
    Operation(
        "p2_freq_extract_10",
        "plink2",
        "list_filter",
        "--freq --extract <10 SNPs>",
        ("--freq", "--extract", "${aux:extract_10}"),
        aux=("extract_10",),
    ),
    Operation(
        "p2_freq_extract_1k",
        "plink2",
        "list_filter",
        "--freq --extract <1k SNPs>",
        ("--freq", "--extract", "${aux:extract_1k}"),
        aux=("extract_1k",),
    ),
    Operation(
        "p2_freq_extract_100k",
        "plink2",
        "list_filter",
        "--freq --extract <100k SNPs>",
        ("--freq", "--extract", "${aux:extract_100k}"),
        aux=("extract_100k",),
    ),
    # -----------------------------------------------------------------
    # Sample filters
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_keep_10",
        "plink1.9",
        "sample_filter",
        "--freq --keep <10 samples>",
        ("--freq", "--keep", "${aux:keep_10}"),
        aux=("keep_10",),
    ),
    Operation(
        "p19_freq_remove_10",
        "plink1.9",
        "sample_filter",
        "--freq --remove <10 samples>",
        ("--freq", "--remove", "${aux:keep_10}"),
        aux=("keep_10",),
    ),
    Operation(
        "p2_freq_keep_10",
        "plink2",
        "sample_filter",
        "--freq --keep <10 samples>",
        ("--freq", "--keep", "${aux:keep_10}"),
        aux=("keep_10",),
    ),
    # -----------------------------------------------------------------
    # Quality filters (combined with --freq baseline)
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_maf",
        "plink1.9",
        "quality_filter",
        "--freq --maf 0.05",
        ("--freq", "--maf", "0.05"),
    ),
    Operation(
        "p19_freq_geno",
        "plink1.9",
        "quality_filter",
        "--freq --geno 0.05",
        ("--freq", "--geno", "0.05"),
    ),
    Operation(
        "p19_freq_maf_geno",
        "plink1.9",
        "quality_filter",
        "--freq --maf 0.05 --geno 0.05",
        ("--freq", "--maf", "0.05", "--geno", "0.05"),
    ),
    Operation(
        "p2_freq_maf",
        "plink2",
        "quality_filter",
        "--freq --maf 0.05",
        ("--freq", "--maf", "0.05"),
    ),
    # -----------------------------------------------------------------
    # LD / pruning
    # -----------------------------------------------------------------
    Operation(
        "p19_indep_pairwise",
        "plink1.9",
        "ld",
        "--indep-pairwise 50 5 0.1",
        ("--indep-pairwise", "50", "5", "0.1"),
    ),
    Operation(
        "p2_indep_pairwise",
        "plink2",
        "ld",
        "--indep-pairwise 50 5 0.1",
        ("--indep-pairwise", "50", "5", "0.1"),
    ),
    Operation(
        "p19_ld_single_pair",
        "plink1.9",
        "ld",
        "--ld rs100 rs101",
        ("--ld", "rs100", "rs101"),
    ),
    Operation(
        "p19_r2_window",
        "plink1.9",
        "ld",
        "--r2 --ld-window-r2 0.2",
        ("--r2", "--ld-window-r2", "0.2"),
        expensive=True,
    ),
    # -----------------------------------------------------------------
    # PCA / GRM (mostly expensive)
    # -----------------------------------------------------------------
    Operation(
        "p19_pca",
        "plink1.9",
        "pca",
        "--pca 10",
        ("--pca", "10"),
        expensive=True,
    ),
    Operation(
        "p2_pca_approx",
        "plink2",
        "pca",
        "--pca approx 10",
        ("--pca", "approx", "10"),
    ),
    Operation(
        "p19_grm_bin",
        "plink1.9",
        "grm",
        "--make-grm-bin",
        ("--make-grm-bin",),
        expensive=True,
    ),
    Operation(
        "p2_make_king",
        "plink2",
        "grm",
        "--make-king",
        ("--make-king",),
        expensive=True,
    ),
    # -----------------------------------------------------------------
    # GWAS
    # -----------------------------------------------------------------
    Operation(
        "p19_linear",
        "plink1.9",
        "gwas",
        "--linear --pheno <random>",
        ("--linear", "--pheno", "${aux:pheno}", "--allow-no-sex"),
        aux=("pheno",),
    ),
    Operation(
        "p2_glm",
        "plink2",
        "gwas",
        "--glm --pheno <random>",
        ("--glm", "allow-no-covars", "--pheno", "${aux:pheno}"),
        aux=("pheno",),
    ),
)
