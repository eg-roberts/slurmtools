#!/usr/bin/env python3
import re
import os
import sys
import itertools
import platform
import subprocess
from functools import reduce

def expand_range(chars):
    if '-' not in chars:
        return [chars]
    
    start, end = chars.split('-')

    if len(start) != len(end):
        raise Exception("start and end values of range expression must have same number of digits")

    digit_count = len(start)
    start_num = int(start)
    end_num = int(end)
    
    return [str(i).zfill(digit_count) for i in range(start_num, end_num + 1)]

def parse_range_field(chars):
    field_contents = chars.split(',')
    result = []
    
    for item in field_contents:
        result += expand_range(item)
    
    return result
 
def expand_pattern(pattern):
  subpattern_list=[]
  intermediate_result = ''
  is_subpattern=False

  for item in re.split(r"(\[|\])",pattern):
    match item:
      case '[':
        is_subpattern=True
      case ']':
        is_subpattern=False
      case _:
        if is_subpattern:
          intermediate_result += '#' 
          subpattern_list.append(parse_range_field(item))
        else:
          intermediate_result += item
  # if pattern == 'node[0-1]-[0-1]',
  #   intermediate_result should equal 'node#-#'
  #   subpattern_list should equal ['01','01']

  hostname_fragments=re.split('(#)', intermediate_result)
  permutations = list(itertools.product(*subpattern_list))
  # hostname_fragments should equal ['node','#','-','#']
  # permutations should equal ('00','01','10','11')

  names=[] # we will return this
  n_index=0
  for p in permutations:
    names.append('')
    p_index=0
    for f in hostname_fragments:
      if f == '#':
        names[n_index]+=(p[p_index])
        p_index+=1
        if p_index >= len(permutations)-1:
          break
      else:
        names[n_index] += f
    n_index+=1
  #  names should equal ['node0-0','node0-1','node1-0','node1-1']
   
  return names

def run_tests():
    test_cases = {
        "my-node-james": ["my-node-james"],
        "node[0-3]": ["node0", "node1", "node2", "node3"],
        "test--[0,3]": ["test--0", "test--3"],
        "node[4,7-9]": ["node4", "node7", "node8", "node9"],
        "fooq-[0-2]-[4,5]": ["fooq-0-4", "fooq-0-5", "fooq-1-4", "fooq-1-5", "fooq-2-4", "fooq-2-5"],
        "as-sd[000-003]": ["as-sd000", "as-sd001", "as-sd002", "as-sd003"],
        "omma[08-12]": ["omma08", "omma09", "omma10", "omma11", "omma12"],
        "zi[888,988]ng[0,1]": ["zi888ng0", "zi888ng1", "zi988ng0", "zi988ng1"]
    }
    
    for pattern, expected in test_cases.items():
        result = expand_pattern(pattern)
        assert result == expected, f"Failed for {pattern}:\nExpected: {expected}\nGot: {result}"
        print(f"Test passed for: {pattern}")


def show_assigned_gpus(nodename,jobid=False):

    if jobid:
        slurm_job_id = jobid
    else:
        with open("/proc/" + str(os.getpid()) + "/cgroup") as file:
          my_cgroup = file.readline()

        if len(my_cgroup.split('/')) < 4 or my_cgroup.split('/')[2] != "slurmstepd.scope":
          print("This process (pid " + str(os.getpid()) + ") does not appear to be controlled by Slurm.")
          sys.exit(1)
        else:
          slurm_job_id = my_cgroup.split('/')[3].split('_')[1]

    scontrol_command = ['scontrol', '-d', 'show', 'job', slurm_job_id ]

    try:
      scontrol_job_output = subprocess.run(scontrol_command, stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()
    except FileNotFoundError:
      print("Slurm tools do not appear to be installed.  Exiting.")
      sys.exit(2)
    except subprocess.CalledProcessError:
      print("`scontrol` returned an error.  Exiting.")
      sys.exit(3)
    except subprocess.TimeoutExpired:
      print("Timed out waiting for `scontrol` output.  Exiting.")
      sys.exit(3)

    node_resource_allocations = [ line.lstrip() for line in scontrol_job_output if line.lstrip().startswith("Nodes=") ]

    gpu_allocations = {}
    for line in node_resource_allocations:
        node_names = expand_pattern(re.split("(Nodes=)",line)[2].split()[0])
        for name in node_names:
            gpu_allocations.update({ name: ','.join(expand_range(re.split("(Nodes=)",line)[2].split()[3].split("IDX:")[1].split(")")[0])) })

    return gpu_allocations[nodename]

if __name__ == "__main__":
    usage = '''
    To expand a nodename pattern:
      slurmtools.py expand_pattern <PATTERN>
      ( depending on your shell, you may need to enclose <PATTERN> in quotes )

    To report the assigned GPU indices of this process's allocation:
      slurmtools.py show_assigned_gpus

    To report the assigned GPU indices of a job running on a specified node:
      slurmtools.py show_assigned_gpus <NODE> <JOB>
      
    '''

    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

    match sys.argv[1]:
      case 'expand_pattern':
        try:
          print(expand_pattern(sys.argv[2]))
          sys.exit(0)
        except IndexError:
          print(usage)
          sys.exit(1)

      case 'show_assigned_gpus':
        if len(sys.argv) == 2:
          hostname = platform.node().split('.', 1)[0]
          print( show_assigned_gpus(hostname) )
          sys.exit(0)
        else:
          try:
            print( show_assigned_gpus(sys.argv[2],sys.argv[3]) )
            sys.exit(0)
          except IndexError:
            print(usage)
            sys.exit(1)

      case _:
        print(usage)
        sys.exit(1)

    if len(sys.argv) == 1:
        hostname = platform.node().split('.', 1)[0]
        gpus = show_assigned_gpus(hostname)
    elif len(sys.argv) == 3:
        hostname = sys.argv[1]
        jobid = sys.argv[2]
        gpus = show_assigned_gpus(hostname,jobid)
    else:
        print("Usage: show_assigned_gpus.py OR show_assigned_gpus.py <NODE_NAME> <JOB_ID>")
        sys.exit()


    if len(gpus) > 1:
        print(gpus)
    else:
        print(hostname + "does not appear to have any gpus allocated for this job")
