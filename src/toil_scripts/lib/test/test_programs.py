import os
import re

def test_docker_call(tmpdir):
    from toil_scripts.lib.programs import docker_call
    work_dir = str(tmpdir)
    parameter = ['--help']
    tool = 'quay.io/ucsc_cgl/samtools'
    docker_call(work_dir=work_dir, parameters=parameter, tool=tool)
    # Test outfile
    fpath = os.path.join(work_dir, 'test')
    with open(fpath, 'w') as f:
        docker_call(tool='ubuntu', env=dict(foo='bar'), parameters=['printenv', 'foo'], outfile=f)
    assert open(fpath).read() == 'bar\n'

    # Test pipe functionality
    # download ubuntu docker image
    docker_call(work_dir=work_dir, tool="ubuntu")
    command1 = ['head -c 1G /dev/urandom | tee /data/first', 'gzip', 'gunzip', 'md5sum 1>&2']
    command2 = ['md5sum /data/first 1>&2']
    # Test 'pipe-in-single-container' mode
    stdout1 = docker_call(work_dir=work_dir, parameters=command1, tools='ubuntu', check_output=True, return_stderr=True)
    stdout2 = docker_call(work_dir=work_dir, parameters=command2, tool='ubuntu', check_output=True, return_stderr=True)
    test1 = re.findall(r"([a-fA-F\d]{32})", stdout1)
    test2 = re.findall(r"([a-fA-F\d]{32})", stdout2)
    assert test1[0] == test2[0]
