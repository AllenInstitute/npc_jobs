import subprocess


def main():
    subprocess.Popen(
        'huey_consumer np_queuey.hueys.dynamicrouting_behavior_session_mtrain_upload.huey -l logs/huey.log',
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )


if __name__ == '__main__':
    main()
