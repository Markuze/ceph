"""
Mount/unmount a ``kernel`` client.
"""
import contextlib
import logging

from teuthology.misc import deep_merge
from teuthology.exceptions import CommandFailedError
from teuthology import misc
from teuthology.contextutil import MaxWhileTries
from tasks.cephfs.kernel_mount import KernelMount

log = logging.getLogger(__name__)

def collect_kernel_debug_artifacts(remote, client_id, test_dir):
    """
    Collect kernel debug artifacts from /sys/kernel/debug/ceph/

    Finds the GUID.clientid directory and collects various debug files
    including TLS traces and other useful kernel debugging information.
    """
    collected_files = []

    try:
        # Find the ceph debug directory for this client
        # Pattern: /sys/kernel/debug/ceph/{GUID}.client{id}/
        debug_base = "/sys/kernel/debug/ceph"

        # List directories matching pattern for this client
        find_cmd = f"find {debug_base} -name '*client{client_id}' -type d 2>/dev/null | head -1"

        log.info(f"Looking for kernel debug directory for client.{client_id}")
        from io import StringIO
        result = remote.run(
            args=["sudo", "bash", "-c", find_cmd],
            stdout=StringIO(),
            check_status=False
        )

        if result.returncode != 0 or not result.stdout.getvalue().strip():
            log.warning(f"No kernel debug directory found for client.{client_id}")
            return collected_files

        debug_dir = result.stdout.getvalue().strip()
        log.info(f"Found kernel debug directory: {debug_dir}")

        # List of debug files to collect
        debug_files = [
            'cephsan_tls',   # TLS trace data
            'caps',          # Capabilities info
            'dentry_lru',    # Dentry LRU cache info
            'mds_sessions',  # MDS session info
            'osdc',          # OSD client info
            'monc',          # Monitor client info
        ]

        for debug_file in debug_files:
            try:
                source_file = f"{debug_dir}/{debug_file}"
                output_file = f"{test_dir}/kernel_{debug_file}.client.{client_id}.log"

                log.info(f"Collecting kernel debug file: {debug_file}")

                # Check if the debug file exists first
                check_cmd = f"test -f {source_file}"
                check_result = remote.run(
                    args=["sudo", "bash", "-c", check_cmd],
                    check_status=False
                )

                if check_result.returncode != 0:
                    log.debug(f"Debug file {source_file} not found, skipping")
                    continue

                # Use bash -c to handle sudo + redirect properly
                collect_cmd = f"cat {source_file} > {output_file} 2>/dev/null || echo 'Failed to read {source_file}' > {output_file}"

                remote.run(
                    args=["sudo", "bash", "-c", collect_cmd],
                    logger=log.getChild(f'kernel_{debug_file}.client.{client_id}')
                )

                log.info(f"Kernel debug file {debug_file} collected to {output_file}")
                collected_files.append(output_file)

            except Exception as e:
                log.warning(f"Failed to collect kernel debug file {debug_file} for client.{client_id}: {e}")

    except Exception as e:
        log.warning(f"Failed to collect kernel debug artifacts for client.{client_id}: {e}")

    return collected_files


def collect_kernel_tls_trace(remote, client_id, test_dir):
    """
    Collect kernel TLS trace data from /sys/kernel/debug/ceph/

    Finds the GUID.clientid directory and cats the cephsan_tls file
    to a log file with proper sudo handling.

    This is kept for backward compatibility, but now just calls the
    more comprehensive collect_kernel_debug_artifacts function.
    """
    artifacts = collect_kernel_debug_artifacts(remote, client_id, test_dir)
    # Return the TLS trace file if collected
    for artifact in artifacts:
        if 'cephsan_tls' in artifact:
            return artifact
    return None


def copy_artifacts_to_archive(ctx, remote, client_id, collected_files):
    """
    Copy collected artifacts to the test archive for preservation.

    Similar to what quiescer.py does - copies files from the remote test
    directory to the local archive so they get picked up by teuthology.
    """
    if not collected_files:
        return

    try:
        # Get archive path, handle both teuthology runs and vstart
        if hasattr(ctx, 'archive') and ctx.archive:
            archive_path = ctx.archive.strip("/")
        else:
            # For vstart or other local runs, try to create a sensible archive path
            import tempfile
            import os
            archive_path = os.path.join(tempfile.gettempdir(), "teuthology_artifacts")
            os.makedirs(archive_path, exist_ok=True)
            log.info(f"Using local archive path: {archive_path}")

        for remote_file in collected_files:
            if remote_file and remote_file.strip():
                try:
                    # Get just the filename for the archive
                    filename = remote_file.split('/')[-1]
                    archive_file = f"{archive_path}/kclient-{filename}"

                    log.info(f"Copying {remote_file} from client.{client_id} to {archive_file}")

                    # Use misc.get_file to copy from remote to local archive
                    from teuthology import misc
                    file_content = misc.get_file(remote, remote_file, sudo=True)

                    with open(archive_file, 'wb') as f:
                        f.write(file_content)

                    log.info(f"Successfully archived kernel artifact: {archive_file}")

                except Exception as e:
                    log.warning(f"Failed to copy {remote_file} to archive: {e}")

    except Exception as e:
        log.warning(f"Failed to copy artifacts to archive for client.{client_id}: {e}")

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``kernel`` client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on. This lets you e.g. set up one client with
    ``ceph-fuse`` and another with ``kclient``.

    ``brxnet`` should be a Private IPv4 Address range, default range is
    [192.168.0.0/16]

    Example that mounts all clients::

        tasks:
        - ceph:
        - kclient:
        - interactive:
        - brxnet: [192.168.0.0/16]

    Example that uses both ``kclient` and ``ceph-fuse``::

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - kclient: [client.1]
        - interactive:


    Pass a dictionary instead of lists to specify per-client config:

        tasks:
        -kclient:
            client.0:
                debug: true
                mntopts: ["nowsync"]

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Mounting kernel clients...')

    if config is None:
        ids = misc.all_roles_of_type(ctx.cluster, 'client')
        client_roles = [f'client.{id_}' for id_ in ids]
        config = dict([r, dict()] for r in client_roles)
    elif isinstance(config, list):
        client_roles = config
        config = dict([r, dict()] for r in client_roles)
    elif isinstance(config, dict):
        client_roles = filter(lambda x: 'client.' in x, config.keys())
    else:
        raise ValueError(f"Invalid config object: {config} ({config.__class__})")
    log.info(f"config is {config}")

    clients = list(misc.get_clients(ctx=ctx, roles=client_roles))

    test_dir = misc.get_testdir(ctx)

    for id_, remote in clients:
        KernelMount.cleanup_stale_netnses_and_bridge(remote)

    mounts = {}
    overrides = ctx.config.get('overrides', {}).get('kclient', {})
    top_overrides = dict(filter(lambda x: 'client.' not in x[0], overrides.items()))
    for id_, remote in clients:
        entity = f"client.{id_}"
        client_config = config.get(entity)
        if client_config is None:
            client_config = {}
        # top level overrides
        deep_merge(client_config, top_overrides)
        # mount specific overrides
        client_config_overrides = overrides.get(entity)
        deep_merge(client_config, client_config_overrides)
        log.info(f"{entity} config is {client_config}")

        cephfs_name = client_config.get("cephfs_name")
        if config.get("disabled", False) or not client_config.get('mounted', True):
            continue

        kernel_mount = KernelMount(
            ctx=ctx,
            test_dir=test_dir,
            client_id=id_,
            client_remote=remote,
            brxnet=ctx.teuthology_config.get('brxnet', None),
            client_config=client_config,
            cephfs_name=cephfs_name)

        mounts[id_] = kernel_mount
        # Store debug flag for use during unmount
        kernel_mount._debug_enabled = client_config.get('debug', False)

        if client_config.get('debug', False):
            remote.run(args=["sudo", "bash", "-c", "echo 'module ceph +p' > /sys/kernel/debug/dynamic_debug/control"])
            remote.run(args=["sudo", "bash", "-c", "echo 'module libceph +p' > /sys/kernel/debug/dynamic_debug/control"])

        kernel_mount.mount(mntopts=client_config.get('mntopts', []))

    def umount_all():
        log.info('Unmounting kernel clients...')

        # Collect kernel debug artifacts before unmounting if debug is enabled
        for id_, mount in mounts.items():
            if getattr(mount, '_debug_enabled', False) and mount.is_mounted():
                log.info(f"Collecting kernel debug artifacts for client.{id_}")
                collected_files = collect_kernel_debug_artifacts(mount.client_remote, id_, test_dir)
                if collected_files:
                    # Copy the collected artifacts to the test archive
                    copy_artifacts_to_archive(ctx, mount.client_remote, id_, collected_files)

        forced = False
        for mount in mounts.values():
            if mount.is_mounted():
                try:
                    mount.umount()
                except (CommandFailedError, MaxWhileTries):
                    log.warning("Ordinary umount failed, forcing...")
                    forced = True
                    mount.umount_wait(force=True)

        for id_, remote in clients:
            KernelMount.cleanup_stale_netnses_and_bridge(remote)

        return forced

    ctx.mounts = mounts
    try:
        yield mounts
    except:
        umount_all()  # ignore forced retval, we are already in error handling
    finally:

        forced = umount_all()
        if forced:
            # The context managers within the kclient manager worked (i.e.
            # the test workload passed) but for some reason we couldn't
            # umount, so turn this into a test failure.
            raise RuntimeError("Kernel mounts did not umount cleanly")
