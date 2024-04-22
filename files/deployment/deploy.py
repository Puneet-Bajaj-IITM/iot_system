import os

def create_deployment(topic):
    with open('data_receiver_deployment.yaml', 'r') as f:
        deployment_template = f.read()

    deployment_manifest = deployment_template.replace('{{ topic }}', topic)

    with open(f'data_receiver_deployment_{topic}.yaml', 'w') as f:
        f.write(deployment_manifest)

    with open('data_receiver_hpa.yaml', 'r') as f:
        hpa_template = f.read()

    hpa_manifest = hpa_template.replace('{{ topic }}', topic)

    with open(f'data_receiver_hpa_{topic}.yaml', 'w') as f:
        f.write(hpa_manifest)

    os.system(f'kubectl apply -f data_receiver_deployment_{topic}.yaml')
    os.system(f'kubectl apply -f data_receiver_hpa_{topic}.yaml')

def create_cronjob(topic):
    with open('kaf_cronjob.yaml', 'r') as f:
        deployment_template = f.read()

    deployment_manifest = deployment_template.replace('{{ topic }}', topic)

    with open(f'kaf_cronjob_{topic}.yaml', 'w') as f:
        f.write(deployment_manifest)

    os.system(f'kubectl apply -f kaf_cronjob_{topic}.yaml')

def main():
    topics = ["topic1", "topic2", "topic3"]  # List of topics you have
    for topic in topics:
        create_deployment(topic)
        create_cronjob(topic)

if __name__ == "__main__":
    main()
