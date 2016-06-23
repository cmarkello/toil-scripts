import os
import subprocess
import logging
from bd2k.util.exceptions import panic
from toil_scripts.lib.toillib import *

_log = logging.getLogger(__name__)


def mock_mode():
    """
    Checks whether the ADAM_GATK_MOCK_MODE environment variable is set.
    In mock mode, all docker calls other than those to spin up and submit jobs to the spark cluster
    are stubbed out and dummy files are used as inputs and outputs.
    """
    return True if int(os.environ.get('TOIL_SCRIPTS_MOCK_MODE', '0')) else False


def docker_call(tool='',
                tools=[],
                parameters=None,
                work_dir='.',
                rm=True,
                env=None,
                outfile=None,
                inputs=None,
                outputs=None,
                docker_parameters=None,
                check_output=False,
                return_stderr=False,
                mock=None):
    """
    Calls Docker, passing along parameters and tool.

    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools) 
    :param list[str] tools: str list of names of the Docker images and order to be used in
                     adding piped commands to docker. (e.g. ['quay.io/ucsc_cgl/samtools', 'ubuntu'])
    :param list[str] parameters: Command line arguments to be passed to the tool
    :param str work_dir: Directory to mount into the container via `-v`. Destination convention is /data
    :param bool rm: Set to True to pass `--rm` flag.
    :param dict[str,str] env: Environment variables to be added (e.g. dict(JAVA_OPTS='-Xmx15G'))
    :param bool sudo: If True, prepends `sudo` to the docker call
    :param file outfile: Pipe output of Docker call to file handle
    :param list[str] inputs: A list of the input files.
    :param dict[str,str] outputs: A dictionary containing the outputs files as keys with either None
                                  or a url. The value is only used if mock=True
    :param dict[str,str] docker_parameters: Parameters to pass to docker
    :param bool check_output: When True, this function returns docker's output
    :param bool return_stderr: When True, this function includes stderr in docker's output
    :param bool mock: Whether to run in mock mode. If this variable is unset, its value will be determined by
                      the environment variable.

    Piping docker commands can be done in one of two ways depending on use case:
    Running a pipe in docker in 'pipe-in-single-container' mode produces command structure
        docker '... | ... | ...' where each '...' command corresponds to each element in the 'parameters'
        argument that uses a docker container. This is the most efficient method if you want to run a pipe of
        commands where each command uses the same docker container.
    
    Running a pipe in docker in 'pipe-of-containers' mode produces command structure
        docker '...' | docker '...' | docker '...' where each '...' command corresponds to each element in
        the 'parameters' argument that uses a docker container and each 'docker' tool in the pipe
        corresponds to each element in the 'tool' argument
    
    Examples for running command 'head -c 1M </dev/urandom | tee >(md5sum 1>&2) | gzip | gunzip | md5sum 1>&2':
        Running 'pipe-in-single-container' mode:
            command= ['head -c 1M /dev/urandom | tee >(md5sum 1>&2)', 'gzip', 'gunzip', 'md5sum 1>&2']
            work_dir=curr_work_dir
            docker_tools=['ubuntu']
            stdout = docker_call(work_dir=docker_work_dir, parameters=command, tool=docker_tools, check_output=True)
        
        Running 'pipe-of-containers' mode:
            command= ['head -c 1M /dev/urandom | tee >(md5sum 1>&2)', 'gzip', 'gunzip', 'md5sum 1>&2']
            work_dir=curr_work_dir
            docker_tools=['ubuntu', 'ubuntu', 'ubuntu', 'ubuntu']
            stdout = docker_call(work_dir=docker_work_dir, parameters=command, tool=docker_tools, check_output=True)
    """
    from toil_scripts.lib.urls import download_url

    if mock is None:
        mock = mock_mode()
    if parameters is None:
        parameters = []
    if inputs is None:
        inputs = []
    if outputs is None:
        outputs = {}

    for filename in inputs:
        assert(os.path.isfile(os.path.join(work_dir, filename)))

    if mock:
        for filename, url in outputs.items():
            file_path = os.path.join(work_dir, filename)
            if url is None:
                # create mock file
                if not os.path.exists(file_path):
                    f = open(file_path, 'w')
                    f.write("contents") # FIXME
                    f.close()

            else:
                file_path = os.path.join(work_dir, filename)
                if not os.path.exists(file_path):
                    outfile = download_url(url, work_dir=work_dir, name=filename)
                assert os.path.exists(file_path)
        return
    
    base_docker_call = ['docker', 'run',
                        '--log-driver=none',
                        '-v', '{}:/data'.format(os.path.abspath(work_dir))]
    if rm:
        base_docker_call.append('--rm')
    if env:
        for e, v in env.iteritems():
            base_docker_call.extend(['-e', '{}={}'.format(e, v)])
    
    if docker_parameters:
        base_docker_call += docker_parameters
   
    docker_call = []
    
    run_pipe = False

    if bool(tools) == bool(tool):
        raise Exception('Either "tool" or "tools" must contain a value, but not both.')
    if not tools:
        tools = [ tool ]
    else:
        run_pipe = True
 
    # Pipe functionality
    #   each element in the parameters list must represent a sub-pipe command
    shell_flag = True      # Flag for running subprocess with string command or list command
    if run_pipe:
        command_list = []
        if len(tools) > 1:
            # If tool is a list containing multiple docker container name strings
            #   then format the docker call in the 'pipe-of-containers' mode
            docker_call.extend(base_docker_call + ['--entrypoint /bin/bash', tools[0], '-c \'{}\''.format(parameters[0])])
            for i in xrange(1, len(tools)):
                docker_call.extend(['|'] + base_docker_call + ['-i --entrypoint /bin/bash', tools[i], '-c \'{}\''.format(parameters[i])])
            docker_call = " ".join(docker_call)
            _log.debug("Calling docker with %s." % docker_call)
            
        elif len(tools) == 1:
            # If tool is a list containing a single docker container name string
            #   then format the docker call in the 'pipe-in-single-container' mode
            docker_call.extend(base_docker_call + ['--entrypoint /bin/bash', tools[0], '-c \'{}\''.format(" | ".join(parameters))])
            docker_call = " ".join(docker_call)
            _log.debug("Calling docker with %s." % docker_call)
             
    else:        
        docker_call = " ".join(base_docker_call + tools + parameters)
        _log.debug("Calling docker with %s." % docker_call)
    
    RealTimeLogger.get().info('RUNNING Docker container(s) with command: {}'.format(docker_call))
    
    try:
        if outfile:
            subprocess.check_call(docker_call, stdout=outfile, shell=shell_flag)
        elif check_output and return_stderr:
            return subprocess.check_output(docker_call, shell=shell_flag, stderr=subprocess.STDOUT)
        elif check_output:
            return subprocess.check_output(docker_call, shell=shell_flag)
        elif return_stderr:
            return subprocess.check_call(docker_call, stderr=subprocess.STDOUT, shell=shell_flag)
        else:
            subprocess.check_call(docker_call, shell=shell_flag)
    # Fix root ownership of output files
    except:
        # Panic avoids hiding the exception raised in the try block
        with panic():
            _fix_permissions(base_docker_call, tools, work_dir)
    else:
        _fix_permissions(base_docker_call, tools, work_dir)

    for filename in outputs.keys():
        if not os.path.isabs(filename):
            filename = os.path.join(work_dir, filename)
        assert(os.path.isfile(filename))


def _fix_permissions(base_docker_call, tools, work_dir):
    """
    Fix permission of a mounted Docker directory by reusing the tool

    :param list base_docker_call: Docker run parameters
    :param str tool: Name of tool
    :param str work_dir: Path of work directory to recursively chown
    """
    base_docker_call.append('--entrypoint=chown')
    stat = os.stat(work_dir)
    command = []
    command_list = []
    for tool in tools:
        command = base_docker_call + [tool] + ['-R', '{}:{}'.format(stat.st_uid, stat.st_gid), '/data']
        command_list.append(command)
    
    for command in command_list:
        subprocess.check_call(command)




