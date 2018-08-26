import subprocess


def get_gce_instance_status(instance_name) -> str:
    """
    Get Google Compute Engine instance status

    :return:
    """
    output = str(subprocess.check_output(['gcloud', 'compute', 'instances', 'list']))
    lines = output.split('\\n')
    for line in lines:
        line_spt = line.split()
        if line_spt[0] == instance_name:
            return line_spt[-1]
    return ''
