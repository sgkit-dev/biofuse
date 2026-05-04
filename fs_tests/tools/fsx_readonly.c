/*
 * fsx_readonly - read-only file system exerciser
 *
 * Implements the core read-validation algorithm of Apple/xfstests fsx
 * for read-only filesystems. fsx upstream assumes a writable filesystem
 * (it builds its in-memory model by writing first), so it cannot run
 * directly against a read-only FUSE mount such as biofuse.
 *
 * This tool:
 *   1. Pre-loads a "good buffer" by reading the entire target file once
 *      via a separate fd kept on the host filesystem (passed as the
 *      second argument). This is the oracle.
 *   2. Opens the target file (under the read-only mount) for reading.
 *   3. Runs N random ops, each one of:
 *        OP_READ    - pread(2) at random (offset, size)
 *        OP_MAPREAD - mmap+memcpy at random (offset, size)
 *      Each op's bytes are compared byte-for-byte against the oracle.
 *   4. Reports any MISMATCH lines and exits non-zero on first divergence.
 *
 * Output format matches fsx for grep-ability:
 *   "READ ... 0x... 0x... 0x..."     - normal op log
 *   "MAPREAD ... 0x... 0x... 0x..."
 *   "MISMATCH at offset=0x... size=0x..."
 *   "All <N> operations completed A-OK!"  - on success
 *
 * Usage: fsx_readonly <oracle_path> <target_path> <N> <seed> [<max_op_size>]
 *
 * The oracle MUST contain the exact bytes the target file is expected to
 * return. The harness arranges this by pointing oracle at the backing
 * file under fs_tests/.cache and target at the same file via the FUSE
 * mount.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static const size_t DEFAULT_MAX_OP_SIZE = 1 << 20; /* 1 MiB */

static void die(const char *msg)
{
	perror(msg);
	exit(2);
}

static ssize_t read_full(int fd, void *buf, size_t count)
{
	size_t total = 0;
	char *p = buf;
	while (total < count) {
		ssize_t n = pread(fd, p + total, count - total, total);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (n == 0)
			break;
		total += (size_t)n;
	}
	return (ssize_t)total;
}

int main(int argc, char **argv)
{
	if (argc < 5 || argc > 6) {
		fprintf(stderr,
			"usage: %s <oracle_path> <target_path> <N> <seed> [<max_op_size>]\n",
			argv[0]);
		return 2;
	}

	const char *oracle_path = argv[1];
	const char *target_path = argv[2];
	long n_ops = strtol(argv[3], NULL, 0);
	unsigned seed = (unsigned)strtoul(argv[4], NULL, 0);
	size_t max_op_size = (argc == 6) ? (size_t)strtoul(argv[5], NULL, 0)
					 : DEFAULT_MAX_OP_SIZE;

	if (n_ops <= 0) {
		fprintf(stderr, "N must be positive\n");
		return 2;
	}
	if (max_op_size == 0) {
		fprintf(stderr, "max_op_size must be positive\n");
		return 2;
	}

	int oracle_fd = open(oracle_path, O_RDONLY | O_CLOEXEC);
	if (oracle_fd < 0)
		die("open oracle");
	struct stat oracle_st;
	if (fstat(oracle_fd, &oracle_st) < 0)
		die("fstat oracle");
	off_t fsize = oracle_st.st_size;
	if (fsize <= 0) {
		fprintf(stderr, "oracle file is empty\n");
		return 2;
	}

	int target_fd = open(target_path, O_RDONLY | O_CLOEXEC);
	if (target_fd < 0)
		die("open target");
	struct stat target_st;
	if (fstat(target_fd, &target_st) < 0)
		die("fstat target");
	if (target_st.st_size != fsize) {
		fprintf(stderr,
			"size mismatch: oracle=%lld target=%lld\n",
			(long long)fsize, (long long)target_st.st_size);
		return 1;
	}

	char *oracle_buf = malloc((size_t)fsize);
	if (!oracle_buf)
		die("malloc oracle_buf");
	if (read_full(oracle_fd, oracle_buf, (size_t)fsize) != (ssize_t)fsize) {
		fprintf(stderr, "short read on oracle\n");
		return 2;
	}
	close(oracle_fd);

	void *target_map = mmap(NULL, (size_t)fsize, PROT_READ, MAP_PRIVATE,
				target_fd, 0);
	if (target_map == MAP_FAILED) {
		fprintf(stderr,
			"mmap failed (errno=%d %s); MAPREAD ops will be skipped\n",
			errno, strerror(errno));
		target_map = NULL;
	}

	char *test_buf = malloc(max_op_size);
	if (!test_buf)
		die("malloc test_buf");

	srandom(seed);
	long mismatches = 0;
	long completed = 0;
	long short_reads = 0;
	for (long i = 0; i < n_ops; i++) {
		off_t off = (off_t)(((uint64_t)random() << 31) ^ random()) % fsize;
		size_t size = ((size_t)random() % max_op_size) + 1;
		if (size > (size_t)(fsize - off))
			size = (size_t)(fsize - off);
		int do_mmap = (target_map != NULL) && ((random() & 1) == 0);

		if (do_mmap) {
			memcpy(test_buf, (char *)target_map + off, size);
			if (memcmp(test_buf, oracle_buf + off, size) != 0) {
				mismatches++;
				printf("MAPREAD MISMATCH at offset=0x%llx size=0x%lx\n",
					(long long)off, (long)size);
				break;
			}
		} else {
			ssize_t n = pread(target_fd, test_buf, size, off);
			if (n < 0) {
				fprintf(stderr,
					"READ pread failed at offset=0x%llx size=0x%lx errno=%d %s\n",
					(long long)off, (long)size, errno,
					strerror(errno));
				mismatches++;
				break;
			}
			if ((size_t)n != size)
				short_reads++;
			if (memcmp(test_buf, oracle_buf + off, (size_t)n) != 0) {
				mismatches++;
				printf("READ MISMATCH at offset=0x%llx size=0x%lx\n",
					(long long)off, (long)size);
				break;
			}
		}
		completed++;
	}

	if (target_map)
		munmap(target_map, (size_t)fsize);
	close(target_fd);
	free(test_buf);
	free(oracle_buf);

	printf("Completed %ld of %ld operations (mismatches=%ld short_reads=%ld)\n",
		completed, n_ops, mismatches, short_reads);
	if (mismatches == 0 && completed == n_ops) {
		printf("All %ld operations completed A-OK!\n", n_ops);
		return 0;
	}
	return 1;
}
