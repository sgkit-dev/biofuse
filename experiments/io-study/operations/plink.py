"""plink1.9 / plink2 operations.

Mirrors the 43 ops the original IO study profiled. ``--bfile`` and
``--out`` are now explicit in the argv template (they used to be
injected by the harness).
"""

from .base import Operation

_BFILE = ("--bfile", "${prefix}", "--out", "${out}")


OPERATIONS: tuple[Operation, ...] = (
    # -----------------------------------------------------------------
    # Whole-genome scans (no input restriction)
    # -----------------------------------------------------------------
    Operation(
        "p19_freq",
        "plink1.9",
        "scan",
        "--freq (full scan)",
        (*_BFILE, "--freq"),
    ),
    Operation(
        "p19_missing",
        "plink1.9",
        "scan",
        "--missing",
        (*_BFILE, "--missing"),
    ),
    Operation(
        "p19_hardy",
        "plink1.9",
        "scan",
        "--hardy",
        (*_BFILE, "--hardy"),
    ),
    Operation("p19_het", "plink1.9", "scan", "--het", (*_BFILE, "--het")),
    Operation(
        "p2_freq",
        "plink2",
        "scan",
        "--freq (full scan)",
        (*_BFILE, "--freq"),
    ),
    Operation(
        "p2_missing",
        "plink2",
        "scan",
        "--missing",
        (*_BFILE, "--missing"),
    ),
    Operation(
        "p2_hardy",
        "plink2",
        "scan",
        "--hardy",
        (*_BFILE, "--hardy"),
    ),
    Operation("p2_het", "plink2", "scan", "--het", (*_BFILE, "--het")),
    # -----------------------------------------------------------------
    # Output operations (read + write)
    # -----------------------------------------------------------------
    Operation(
        "p19_make_bed", "plink1.9", "output", "--make-bed", (*_BFILE, "--make-bed")
    ),
    Operation("p2_make_bed", "plink2", "output", "--make-bed", (*_BFILE, "--make-bed")),
    Operation(
        "p19_recode_A",
        "plink1.9",
        "output",
        "--recode A",
        (*_BFILE, "--recode", "A"),
    ),
    Operation(
        "p2_export_A",
        "plink2",
        "output",
        "--export A",
        (*_BFILE, "--export", "A"),
    ),
    # -----------------------------------------------------------------
    # Range filters: chromosome
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_chr1",
        "plink1.9",
        "range_filter",
        "--freq --chr 1",
        (*_BFILE, "--freq", "--chr", "1"),
    ),
    Operation(
        "p19_freq_chr1_3",
        "plink1.9",
        "range_filter",
        "--freq --chr 1-3",
        (*_BFILE, "--freq", "--chr", "1-3"),
    ),
    Operation(
        "p2_freq_chr1",
        "plink2",
        "range_filter",
        "--freq --chr 1",
        (*_BFILE, "--freq", "--chr", "1"),
    ),
    Operation(
        "p2_freq_chr1_3",
        "plink2",
        "range_filter",
        "--freq --chr 1-3",
        (*_BFILE, "--freq", "--chr", "1-3"),
    ),
    # -----------------------------------------------------------------
    # Range filters: bp position
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_bp_window_chr1",
        "plink1.9",
        "range_filter",
        "--freq --chr 1 --from-bp 0 --to-bp 100000",
        (*_BFILE, "--freq", "--chr", "1", "--from-bp", "0", "--to-bp", "100000"),
    ),
    Operation(
        "p2_freq_bp_window_chr1",
        "plink2",
        "range_filter",
        "--freq --chr 1 --from-bp 0 --to-bp 100000",
        (*_BFILE, "--freq", "--chr", "1", "--from-bp", "0", "--to-bp", "100000"),
    ),
    # -----------------------------------------------------------------
    # Range filters: SNP-id range
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_snp_range",
        "plink1.9",
        "range_filter",
        "--freq --snp rs500000 --window 1000",
        (*_BFILE, "--freq", "--snp", "rs500000", "--window", "1000"),
    ),
    # -----------------------------------------------------------------
    # SNP-list filters
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_extract_10",
        "plink1.9",
        "list_filter",
        "--freq --extract <10 SNPs>",
        (*_BFILE, "--freq", "--extract", "${aux:extract_10}"),
        aux=("extract_10",),
    ),
    Operation(
        "p19_freq_extract_1k",
        "plink1.9",
        "list_filter",
        "--freq --extract <1k SNPs>",
        (*_BFILE, "--freq", "--extract", "${aux:extract_1k}"),
        aux=("extract_1k",),
    ),
    Operation(
        "p19_freq_extract_100k",
        "plink1.9",
        "list_filter",
        "--freq --extract <100k SNPs>",
        (*_BFILE, "--freq", "--extract", "${aux:extract_100k}"),
        aux=("extract_100k",),
    ),
    Operation(
        "p19_freq_exclude_10",
        "plink1.9",
        "list_filter",
        "--freq --exclude <10 SNPs>",
        (*_BFILE, "--freq", "--exclude", "${aux:extract_10}"),
        aux=("extract_10",),
    ),
    Operation(
        "p2_freq_extract_10",
        "plink2",
        "list_filter",
        "--freq --extract <10 SNPs>",
        (*_BFILE, "--freq", "--extract", "${aux:extract_10}"),
        aux=("extract_10",),
    ),
    Operation(
        "p2_freq_extract_1k",
        "plink2",
        "list_filter",
        "--freq --extract <1k SNPs>",
        (*_BFILE, "--freq", "--extract", "${aux:extract_1k}"),
        aux=("extract_1k",),
    ),
    Operation(
        "p2_freq_extract_100k",
        "plink2",
        "list_filter",
        "--freq --extract <100k SNPs>",
        (*_BFILE, "--freq", "--extract", "${aux:extract_100k}"),
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
        (*_BFILE, "--freq", "--keep", "${aux:keep_10}"),
        aux=("keep_10",),
    ),
    Operation(
        "p19_freq_remove_10",
        "plink1.9",
        "sample_filter",
        "--freq --remove <10 samples>",
        (*_BFILE, "--freq", "--remove", "${aux:keep_10}"),
        aux=("keep_10",),
    ),
    Operation(
        "p2_freq_keep_10",
        "plink2",
        "sample_filter",
        "--freq --keep <10 samples>",
        (*_BFILE, "--freq", "--keep", "${aux:keep_10}"),
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
        (*_BFILE, "--freq", "--maf", "0.05"),
    ),
    Operation(
        "p19_freq_geno",
        "plink1.9",
        "quality_filter",
        "--freq --geno 0.05",
        (*_BFILE, "--freq", "--geno", "0.05"),
    ),
    Operation(
        "p19_freq_maf_geno",
        "plink1.9",
        "quality_filter",
        "--freq --maf 0.05 --geno 0.05",
        (*_BFILE, "--freq", "--maf", "0.05", "--geno", "0.05"),
    ),
    Operation(
        "p2_freq_maf",
        "plink2",
        "quality_filter",
        "--freq --maf 0.05",
        (*_BFILE, "--freq", "--maf", "0.05"),
    ),
    # -----------------------------------------------------------------
    # LD / pruning
    # -----------------------------------------------------------------
    Operation(
        "p19_indep_pairwise",
        "plink1.9",
        "ld",
        "--indep-pairwise 50 5 0.1",
        (*_BFILE, "--indep-pairwise", "50", "5", "0.1"),
    ),
    Operation(
        "p2_indep_pairwise",
        "plink2",
        "ld",
        "--indep-pairwise 50 5 0.1",
        (*_BFILE, "--indep-pairwise", "50", "5", "0.1"),
    ),
    Operation(
        "p19_ld_single_pair",
        "plink1.9",
        "ld",
        "--ld rs100 rs101",
        (*_BFILE, "--ld", "rs100", "rs101"),
    ),
    Operation(
        "p19_r2_window",
        "plink1.9",
        "ld",
        "--r2 --ld-window-r2 0.2",
        (*_BFILE, "--r2", "--ld-window-r2", "0.2"),
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
        (*_BFILE, "--pca", "10"),
        expensive=True,
    ),
    Operation(
        "p2_pca_approx",
        "plink2",
        "pca",
        "--pca approx 10",
        (*_BFILE, "--pca", "approx", "10"),
    ),
    Operation(
        "p19_grm_bin",
        "plink1.9",
        "grm",
        "--make-grm-bin",
        (*_BFILE, "--make-grm-bin"),
        expensive=True,
    ),
    Operation(
        "p2_make_king",
        "plink2",
        "grm",
        "--make-king",
        (*_BFILE, "--make-king"),
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
        (*_BFILE, "--linear", "--pheno", "${aux:pheno}", "--allow-no-sex"),
        aux=("pheno",),
    ),
    Operation(
        "p2_glm",
        "plink2",
        "gwas",
        "--glm --pheno <random>",
        (*_BFILE, "--glm", "allow-no-covars", "--pheno", "${aux:pheno}"),
        aux=("pheno",),
    ),
    # -----------------------------------------------------------------
    # Threading variants: every scan op plus every heavy compute op,
    # rerun with --threads 8. Compared against the default-threads
    # baseline above to detect concurrent .bed readers (or confirm
    # plink stays single-stream under threading).
    # -----------------------------------------------------------------
    Operation(
        "p19_freq_t8",
        "plink1.9",
        "scan_t8",
        "--freq --threads 8",
        (*_BFILE, "--freq", "--threads", "8"),
    ),
    Operation(
        "p19_missing_t8",
        "plink1.9",
        "scan_t8",
        "--missing --threads 8",
        (*_BFILE, "--missing", "--threads", "8"),
    ),
    Operation(
        "p19_hardy_t8",
        "plink1.9",
        "scan_t8",
        "--hardy --threads 8",
        (*_BFILE, "--hardy", "--threads", "8"),
    ),
    Operation(
        "p19_het_t8",
        "plink1.9",
        "scan_t8",
        "--het --threads 8",
        (*_BFILE, "--het", "--threads", "8"),
    ),
    Operation(
        "p2_freq_t8",
        "plink2",
        "scan_t8",
        "--freq --threads 8",
        (*_BFILE, "--freq", "--threads", "8"),
    ),
    Operation(
        "p2_missing_t8",
        "plink2",
        "scan_t8",
        "--missing --threads 8",
        (*_BFILE, "--missing", "--threads", "8"),
    ),
    Operation(
        "p2_hardy_t8",
        "plink2",
        "scan_t8",
        "--hardy --threads 8",
        (*_BFILE, "--hardy", "--threads", "8"),
    ),
    Operation(
        "p2_het_t8",
        "plink2",
        "scan_t8",
        "--het --threads 8",
        (*_BFILE, "--het", "--threads", "8"),
    ),
    Operation(
        "p19_make_bed_t8",
        "plink1.9",
        "output_t8",
        "--make-bed --threads 8",
        (*_BFILE, "--make-bed", "--threads", "8"),
    ),
    Operation(
        "p2_make_bed_t8",
        "plink2",
        "output_t8",
        "--make-bed --threads 8",
        (*_BFILE, "--make-bed", "--threads", "8"),
    ),
    Operation(
        "p19_recode_A_t8",
        "plink1.9",
        "output_t8",
        "--recode A --threads 8",
        (*_BFILE, "--recode", "A", "--threads", "8"),
    ),
    Operation(
        "p2_export_A_t8",
        "plink2",
        "output_t8",
        "--export A --threads 8",
        (*_BFILE, "--export", "A", "--threads", "8"),
    ),
    Operation(
        "p19_indep_pairwise_t8",
        "plink1.9",
        "ld_t8",
        "--indep-pairwise 50 5 0.1 --threads 8",
        (*_BFILE, "--indep-pairwise", "50", "5", "0.1", "--threads", "8"),
    ),
    Operation(
        "p2_indep_pairwise_t8",
        "plink2",
        "ld_t8",
        "--indep-pairwise 50 5 0.1 --threads 8",
        (*_BFILE, "--indep-pairwise", "50", "5", "0.1", "--threads", "8"),
    ),
    Operation(
        "p19_r2_window_t8",
        "plink1.9",
        "ld_t8",
        "--r2 --ld-window-r2 0.2 --threads 8",
        (*_BFILE, "--r2", "--ld-window-r2", "0.2", "--threads", "8"),
        expensive=True,
    ),
    Operation(
        "p19_pca_t8",
        "plink1.9",
        "pca_t8",
        "--pca 10 --threads 8",
        (*_BFILE, "--pca", "10", "--threads", "8"),
        expensive=True,
    ),
    Operation(
        "p2_pca_approx_t8",
        "plink2",
        "pca_t8",
        "--pca approx 10 --threads 8",
        (*_BFILE, "--pca", "approx", "10", "--threads", "8"),
    ),
    Operation(
        "p19_grm_bin_t8",
        "plink1.9",
        "grm_t8",
        "--make-grm-bin --threads 8",
        (*_BFILE, "--make-grm-bin", "--threads", "8"),
        expensive=True,
    ),
    Operation(
        "p2_make_king_t8",
        "plink2",
        "grm_t8",
        "--make-king --threads 8",
        (*_BFILE, "--make-king", "--threads", "8"),
        expensive=True,
    ),
    Operation(
        "p19_linear_t8",
        "plink1.9",
        "gwas_t8",
        "--linear --pheno <random> --threads 8",
        (
            *_BFILE,
            "--linear",
            "--pheno",
            "${aux:pheno}",
            "--allow-no-sex",
            "--threads",
            "8",
        ),
        aux=("pheno",),
    ),
    Operation(
        "p2_glm_t8",
        "plink2",
        "gwas_t8",
        "--glm --pheno <random> --threads 8",
        (
            *_BFILE,
            "--glm",
            "allow-no-covars",
            "--pheno",
            "${aux:pheno}",
            "--threads",
            "8",
        ),
        aux=("pheno",),
    ),
)
