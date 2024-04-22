import subprocess

topics = ["topic1", "topic2", "topic3"]  # List of topics to subscribe to

for topic in topics:
    subprocess.run(["kubectl", "create", "deployment", f"data-receiver-{topic}", "--image=your-docker-image-for-data-receiver", "--", "python", "data_receiver.py", topic])
