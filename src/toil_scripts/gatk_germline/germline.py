#!/usr/bin/env python2.7

""" 
GATK HaplotypeCaller Genotyping Mode

            Tree Structure of GATK Pipeline
            0 --> 1 --> 2 --> 3 --> 4
                                    |
                                    5
                                   / \
                                  6   7
                                 /     \
                                8       9
0 = Start Node
1 = Download Reference
2 = Index Reference
3 = Reference Dictionary
4 = Index Samples
5 = HaplotypeCaller SNP & Indel
6 = VariantRecalibrator SNPs
7 = VariantRecalibrator Indels
8 = ApplyRecalibration SNPs
9 = ApplyRecalibration Indels

===================================================================
:Dependencies:
curl            - apt-get install curl
docker          - apt-get install docker (or 'docker.io' for linux)
toil            - pip install --pre toil
"""
from __future__ import print_function
import argparse
import shutil
import os
import multiprocessing
import sys
from collections import OrderedDict
from toil.job import Job

from toil_scripts.lib.files import mkdir_p
from toil_scripts.lib.urls import download_url_job, s3am_upload
from toil_scripts.lib.programs import docker_call

def build_parser():
    """
    Create parser object containing necessary input files
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--reference', required=True, help="Reference Genome URL")
    parser.add_argument('-f', '--config', required=True, help="Each line contains (CSV): UUID,Normal_URL,Tumor_URL")
    parser.add_argument('-p', '--phase', required=True, help='1000G_phase1.indels.b37.vcf URL')
    parser.add_argument('-m', '--mills', required=True, help='Mills_and_1000G_gold_standard.indels.b37.vcf URL')
    parser.add_argument('-d', '--dbsnp', required=True, help='dbsnp_137.b37.vcf URL')
    parser.add_argument('-n', '--omni', required=True, help='1000G_omni.5.b37.vcf URL')
    parser.add_argument('-t', '--hapmap', required=True, help='hapmap_3.3.b37.vcf URL')
    parser.add_argument('-o', '--output_dir', default="./data", help='Full path to final output dir')
    parser.add_argument('-se', '--file_size', default='100G', help='Approximate input file size. Should be given as %d[TGMK], e.g., for a 100 gigabyte file, use --file_size 100G')
    parser.add_argument('-x', '--suffix', default="", help='additional suffix, if any')
    return parser

# Convenience functions
def get_files_from_filestore(job, work_dir, ids, *filenames):
    """
     Puts a working copy of a file from the fileStore into the working directory.

    :param job: Job instance
    :param work_dir: working directory, str
    :param ids: dictionary of shared file ids
    :param filenames: remaining arguments are filenames

    :returns: list of paths to files
    """
    paths = OrderedDict()
    for filename in filenames:
        if not os.path.exists(os.path.join(work_dir, filename)):
            file_path = job.fileStore.readGlobalFile(ids[filename], os.path.join(work_dir, filename))
        else:
            file_path = os.path.join(work_dir, filename)
        paths[filename] = file_path

    return paths.values()


def write_to_filestore(job, work_dir, ids, *filenames):
    """
    Given one or more file names in working directory, writes
    files to filestore and stores the filestore promise in a dictionary

    :param job: Job instance
    :param work_dir: working directory
    :param ids: shared file promises, dict
    :param filenames: remaining arguments are keys for ids

    :returns: shared ids dictionary
    """
    for filename in filenames:
        ids[filename] = job.fileStore.writeGlobalFile(os.path.join(work_dir, filename))
    return ids


def copy_to_output_dir(work_dir, output_dir, *filenames):
    """`
    Moves files from the working directory to the output directory.

    :param work_dir: the working directory
    :param output_dir: the output directory
    :param filenames: remaining arguments are filenames
    """
    for filename in filenames:
        origin = os.path.join(work_dir, filename)
        dest = os.path.join(output_dir, filename)
        shutil.copy(origin, dest)


def download_shared_files(job, input_args):
    """ 
    Downloads reference data and stores fileStore promises,
    spawns next step in pipeline - reference indexing

    :param job: Job instance
    :param input_args: command line arguments, dict

    :returns: None
    """
    shared_files = ['ref.fa', 'phase.vcf', 'omni.vcf', 'dbsnp.vcf', 'hapmap.vcf', 'mills.vcf']
    shared_ids = {}
    for file_name in shared_files:
        url = input_args[file_name]
        shared_ids[file_name] = job.addChildJobFn(download_url_job, url, file_name, s3_key_path=input_args['ssec']).rv()
    job.addFollowOnJobFn(create_reference_index_hc, shared_ids, input_args)


def create_reference_index_hc(job, shared_ids, input_args):
    """
    Uses samtools to create reference index file in working directory,
    spawns next job in pipeline - create reference dictionary

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    # Unpack convenience variables for job
    work_dir = job.fileStore.getLocalTempDir()
    # Retrieve file path
    faidx_output = os.path.join(work_dir, 'ref.fa.fai')
    # Call: Samtools
    faidx_command = ['faidx', 'ref.fa']
    get_files_from_filestore(job, work_dir, shared_ids, 'ref.fa')
    inputs = ['ref.fa']
    outputs={'ref.fa.fai': None}
    docker_call(work_dir = work_dir,
                parameters = faidx_command,
                tool = 'quay.io/ucsc_cgl/samtools',
                inputs=inputs,
                outputs=outputs)
    # Update fileStore for output
    shared_ids['ref.fa.fai'] = job.fileStore.writeGlobalFile(faidx_output)
    job.addChildJobFn(create_reference_dict_hc, shared_ids, input_args)


def create_reference_dict_hc(job, shared_ids, input_args):
    """
    Uses Picardtools to create sequence dictionary for reference genome.
    Calls next step in pipeline - spawn batch jobs

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    # Unpack convenience variables for job
    work_dir = job.fileStore.getLocalTempDir()
    get_files_from_filestore(job, work_dir, shared_ids, 'ref.fa')
    # Call: picardtools
    picard_output = os.path.join(work_dir, 'ref.dict')
    command = ['CreateSequenceDictionary', 'R=ref.fa', 'O=ref.dict']
    inputs=['ref.fa']
    outputs={picard_output: None}
    docker_call(work_dir = work_dir,
                env={'JAVA_OPTS':'-Xmx%sg' % input_args['memory']},
                parameters = command,
                tool = 'quay.io/ucsc_cgl/picardtools',
                inputs=inputs,
                outputs=outputs)
    # Update fileStore for output
    shared_ids['ref.dict'] = job.fileStore.writeGlobalFile(picard_output)
    job.addChildJobFn(spawn_batch_variant_calling, shared_ids, input_args)


def spawn_batch_variant_calling(job, shared_ids, input_args):
    """
    Runs pipeline on each sample in the config.txt file.

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    # Names for every input file used in the pipeline by each sample
    samples = []
    config = input_args['config']

    # does the config file exist locally? if not, try to read from job store
    if not os.path.exists(config):

        work_dir = job.fileStore.getLocalTempDir()
        config_path = os.path.join(work_dir, 'config.txt')
        job.fileStore.readGlobalFile(config, config_path)
        config = config_path

    with open(config, 'r') as f:
        for line in f.readlines():
            if not line.isspace():
                samples.append(line.strip().split(','))
    for sample in samples:
        job.addChildJobFn(start, shared_ids, input_args, sample)


def start(job, shared_ids, input_args, sample):
    """
    Configures parameters for sample and calls next step in
    pipeline - index sample bam

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    :param sample: tuple with uuid and file url
    """
    uuid, url = sample
    ids = shared_ids.copy()
    # Update input
    input_args['uuid'] = uuid
    # Sample bam file holds a url?
    input_args['bam_url'] = url

    if input_args['output_dir']:
        input_args['output_dir'] = os.path.join(input_args['output_dir'], uuid)

    ids['toil.bam'] = job.addChildJobFn(download_url_job, url, 'toil.bam', s3_key_path=input_args['ssec']).rv()

    if input_args['indexed']:
        ids['toil.bam.bai'] = job.addChildJobFn(download_url_job, "%s.bai" % url, 'toil.bam.bai',
                                                s3_key_path=input_args['ssec']).rv()
        job.addFollowOnJobFn(haplotype_caller, ids, input_args, cores = multiprocessing.cpu_count())
    else:
        job.addFollowOnJobFn(index, ids, input_args)


def index(job, shared_ids, input_args):
    """
    Index sample bam using samtools, calls haplotypeCaller.

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    work_dir = job.fileStore.getLocalTempDir()
    get_files_from_filestore(job, work_dir, shared_ids, 'toil.bam')
    # Call: index the normal.bam
    parameters = ['index', 'toil.bam']
    inputs=['toil.bam']
    outputs={'toil.bam.bai': None}
    docker_call(work_dir = work_dir,
                parameters = parameters,
                tool = 'quay.io/ucsc_cgl/samtools',
                inputs=inputs,
                outputs=outputs)
    # Update FileStore and call child
    output_path = os.path.join(work_dir, 'toil.bam.bai')
    shared_ids['toil.bam.bai'] = job.fileStore.writeGlobalFile(output_path)
    job.addChildJobFn(haplotype_caller, shared_ids, input_args, cores = multiprocessing.cpu_count())


def haplotype_caller(job, shared_ids, input_args):
    """
    Uses GATK HaplotypeCaller to identify SNPs and Indels and writes a gVCF.
    Calls per-sample genotyper to genotype gVCF.

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    work_dir = job.fileStore.getLocalTempDir()
    inputs = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'toil.bam', 'toil.bam.bai']
    get_files_from_filestore(job, work_dir, shared_ids, *inputs)
    output = '%s.raw.BOTH%s.gvcf' % (input_args['uuid'],
                                     input_args['suffix'])

    cores = multiprocessing.cpu_count()
    # Call GATK -- HaplotypeCaller
    command = ['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY', # RISKY! (?) See #189
               '-nct', str(cores),
               '-R', 'ref.fa',
               '-T', 'HaplotypeCaller',
               '--genotyping_mode', 'Discovery',
               '--emitRefConfidence', 'GVCF',
               '-I', 'toil.bam',
               '-o', output,
               '-variant_index_type', 'LINEAR',
               '-variant_index_parameter', '128000',
               '--annotation', 'QualByDepth',
               '--annotation', 'DepthPerSampleHC',
               '--annotation', 'FisherStrand',
               '--annotation', 'ReadPosRankSumTest']
    try:
        outputs={output: None}
        docker_call(work_dir = work_dir,
                    env={'JAVA_OPTS':'-Xmx%sg' % input_args['memory']},
                    parameters = command,
                    tool = 'quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                    inputs=inputs,
                    outputs=outputs)
    except:
        sys.stderr.write("Running haplotype caller with %s in %s failed." % (
            " ".join(command), work_dir))
        raise

    # Update fileStore and spawn child job
    shared_ids[output] = job.fileStore.writeGlobalFile(os.path.join(work_dir, output))

    # upload gvcf
    upload_or_move_hc(work_dir, input_args, output)

    # call variants prior to vqsr
    job.addChildJobFn(genotype_gvcf, shared_ids, input_args, cores = cores)


def genotype_gvcf(job, shared_ids, input_args):
    """
    Genotypes the gVCF generated by the HaplotypeCaller.
    Calls variant quality score recalibration functions.

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """

    work_dir = job.fileStore.getLocalTempDir()
    inputs = ['%s.raw.BOTH%s.gvcf' % (input_args['uuid'],
                                           input_args['suffix']),
                   'ref.fa', 'ref.fa.fai', 'ref.dict']
    get_files_from_filestore(job, work_dir, shared_ids, *inputs)
    output = 'unified.raw.BOTH.gatk.vcf'

    cores = multiprocessing.cpu_count()
    command = ['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY', # RISKY! (?) See #189
               '-nt', str(cores),
               '-R', 'ref.fa',
               '-T', 'GenotypeGVCFs',
               '--variant', '%s.raw.BOTH.gatk.gvcf' % input_args['uuid'],
               '--out', output,
               '-stand_emit_conf', '10.0',
               '-stand_call_conf', '30.0']

    try:
        outputs={output: None}
        docker_call(work_dir = work_dir,
                    env={'JAVA_OPTS':'-Xmx%sg' % input_args['memory']},
                    parameters = command,
                    tool = 'quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                    inputs=inputs,
                    outputs=outputs)
    except:
        sys.stderr.write("Running GenotypeGVCFs with %s in %s failed." % (
            " ".join(command), work_dir))
        raise

    # Update fileStore and spawn child job
    shared_ids[output] = job.fileStore.writeGlobalFile(os.path.join(work_dir, output))

    # run vqsr
    job.addChildJobFn(vqsr_snp, shared_ids, input_args, cores = cores)
    job.addChildJobFn(vqsr_indel, shared_ids, input_args, cores = cores)


def vqsr_snp(job, shared_ids, input_args):
    """
    Variant quality score recalibration for SNP variants.
    Calls next step in pipeline - apply SNP recalibration

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    work_dir = job.fileStore.getLocalTempDir()
    inputs = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'unified.raw.BOTH.gatk.vcf',
              'hapmap.vcf', 'omni.vcf', 'dbsnp.vcf', 'phase.vcf']
    get_files_from_filestore(job, work_dir, shared_ids, *inputs)
    outputs = ['HAPSNP.recal', 'HAPSNP.tranches', 'HAPSNP.plots']

    cores = multiprocessing.cpu_count()
    command = ['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY', # RISKY! (?) See #189
               '-T', 'VariantRecalibrator',
               '-R', 'ref.fa',
               '-input', 'unified.raw.BOTH.gatk.vcf',
               '-nt', str(cores),
               '-resource:hapmap,known=false,training=true,truth=true,prior=15.0', 'hapmap.vcf',
               '-resource:omni,known=false,training=true,truth=false,prior=12.0', 'omni.vcf',
               '-resource:dbsnp,known=true,training=false,truth=false,prior=2.0', 'dbsnp.vcf',
               '-resource:1000G,known=false,training=true,truth=false,prior=10.0', 'phase.vcf',
               '-an', 'QD', '-an', 'DP', '-an', 'FS', '-an', 'ReadPosRankSum',
               '-mode', 'SNP', '-minNumBad', '1000',
               '-recalFile', 'HAPSNP.recal',
               '-tranchesFile', 'HAPSNP.tranches',
               '-rscriptFile', 'HAPSNP.plots']
    outputD={'HAPSNP.recal': None, 'HAPSNP.tranches': None, 'HAPSNP.plots': None}
    docker_call(work_dir = work_dir,
                env={'JAVA_OPTS':'-Xmx%sg' % input_args['memory']},
                parameters = command,
                tool ='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs,
                outputs=outputD)
    shared_ids = write_to_filestore(job, work_dir, shared_ids, *outputs)
    job.addChildJobFn(apply_vqsr_snp, shared_ids, input_args)


def apply_vqsr_snp(job, shared_ids, input_args):
    """
    Apply variant quality score recalibration for SNP variants.
    Writes vcf file to output directory

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    work_dir = job.fileStore.getLocalTempDir()

    uuid = input_args['uuid']
    suffix = input_args['suffix']
    inputs = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'unified.raw.BOTH.gatk.vcf',
                   'HAPSNP.tranches', 'HAPSNP.recal']
    get_files_from_filestore(job, work_dir, shared_ids, *inputs)
    output = '{}.HAPSNP.vqsr.SNP{}.vcf'.format(uuid, suffix)
    command = ['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY', # RISKY! (?) See #189
               '-T', 'ApplyRecalibration',
               '-input', 'unified.raw.BOTH.gatk.vcf',
               '-o', output,
               '-R', 'ref.fa',
               '-nt', '1',
               '-ts_filter_level', '99.0',
               '-tranchesFile', 'HAPSNP.tranches',
               '-recalFile', 'HAPSNP.recal',
               '-mode', 'SNP']
    outputs={output: None}
    docker_call(work_dir = work_dir,
                env={'JAVA_OPTS':'-Xmx%sg' % input_args['memory']},
                parameters = command,
                tool = 'quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs,
                outputs=outputs)

    upload_or_move_hc(work_dir, input_args, output)


def upload_or_move_hc(work_dir, input_args, output):

    # are we moving this into a local dir, or up to s3?
    if input_args['output_dir']:
        # get output path and 
        output_dir = input_args['output_dir']

        mkdir_p(output_dir)
        copy_to_output_dir(work_dir, output_dir, output)

    elif input_args['s3_dir']:

        s3am_upload(fpath=os.path.join(work_dir, output),
                    s3_dir=input_args['s3_dir'],
                    s3_key_path=input_args['ssec'])

    else:

        raise ValueError('No output_directory or s3_dir defined. Cannot determine where to store %s' % output)

# Indel Recalibration
def vqsr_indel(job, shared_ids, input_args):
    """
    Variant quality score recalibration for Indel variants.
    Calls next step in pipeline - apply indel recalibration

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    work_dir = job.fileStore.getLocalTempDir()
    input_files = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'unified.raw.BOTH.gatk.vcf', 'mills.vcf']
    get_files_from_filestore(job, work_dir, shared_ids, *input_files)
    outputs = ['HAPINDEL.recal', 'HAPINDEL.tranches', 'HAPINDEL.plots']
    cores = multiprocessing.cpu_count()
    command = ['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY', # RISKY! (?) See #189
               '-T', 'VariantRecalibrator',
               '-R', 'ref.fa',
               '-input', 'unified.raw.BOTH.gatk.vcf',
               '-nt', str(cores),
               '-resource:mills,known=true,training=true,truth=true,prior=12.0', 'mills.vcf',
               '-an', 'DP', '-an', 'FS', '-an', 'ReadPosRankSum',
               '-mode', 'INDEL',
               '-minNumBad', '1000',
               '-recalFile', 'HAPINDEL.recal',
               '-tranchesFile', 'HAPINDEL.tranches',
               '-rscriptFile', 'HAPINDEL.plots',
               '--maxGaussians', '4']
    inputs=input_files
    outputD={'HAPINDEL.recal': None, 'HAPINDEL.tranches': None, 'HAPINDEL.plots': None}
    docker_call(work_dir = work_dir,
                env={'JAVA_OPTS':'-Xmx%sg' % input_args['memory']},
                parameters = command,
                tool ='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs,
                outputs=outputD)
    shared_ids = write_to_filestore(job, work_dir, shared_ids, *outputs)
    job.addChildJobFn(apply_vqsr_indel, shared_ids, input_args)


def apply_vqsr_indel(job, shared_ids, input_args):
    """
    Apply variant quality score recalibration for indel variants.
    Writes vcf file to output directory

    :param job: Job instance
    :param shared_ids: dictionary of shared file promises
    :param input_args: dictionary of input arguments
    """
    work_dir = job.fileStore.getLocalTempDir()
    uuid = input_args['uuid']
    suffix = input_args['suffix']
    inputs = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'unified.raw.BOTH.gatk.vcf',
                   'HAPINDEL.recal', 'HAPINDEL.tranches', 'HAPINDEL.plots']
    get_files_from_filestore(job, work_dir, shared_ids, *inputs)
    output = '{}.HAPSNP.vqsr.INDEL{}.vcf'.format(uuid, suffix)
    command = ['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY', # RISKY! (?) See #189
               '-T', 'ApplyRecalibration',
               '-input', 'unified.raw.BOTH.gatk.vcf',
               '-o', output,
               '-R', 'ref.fa',
               '-nt', '1',
               '-ts_filter_level', '99.0',
               '-tranchesFile', 'HAPINDEL.tranches',
               '-recalFile', 'HAPINDEL.recal',
               '-mode', 'INDEL']
    outputs={output: None}
    docker_call(work_dir = work_dir,
                env={'JAVA_OPTS':'-Xmx%sg' % input_args['memory']},
                parameters = command,
                tool = 'quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs = inputs,
                outputs = outputs)

    upload_or_move_hc(work_dir, input_args, output)


if __name__ == '__main__':
    args_parser = build_parser()
    Job.Runner.addToilOptions(args_parser)
    args = args_parser.parse_args()

    inputs = {'ref.fa': args.reference,
              'config': args.config,
              'phase.vcf': args.phase,
              'mills.vcf': args.mills,
              'dbsnp.vcf': args.dbsnp,
              'hapmap.vcf': args.hapmap,
              'omni.vcf': args.omni,
              'output_dir': args.output_dir,
              'suffix': args.suffix,
              'uuid': None,
              'file_size': args.file_size,
              'ssec': None,
              'sudo': False,
              'indexed': False, # FIXME: should be parametrized
              'memory': '15'}
    
    Job.Runner.startToil(Job.wrapJobFn(download_shared_files, inputs), args)
