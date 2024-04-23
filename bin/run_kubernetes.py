import stow
from stow.managers.kubernetes import Kubernetes

k8s = Kubernetes('kieran-development-area/exercise-session-processor-55d8c7d8d6-9s72q')

k8s.ls('/home')


# stow.artefact("k8s://kieran-development-area/exercise-session-processor-55d8c7d8d6-vnt88/home/esp")

# stow.get(
#     "k8s://kieran-development-area/exercise-session-processor-55d8c7d8d6-vnt88/home/esp",
#     "esp",
#     overwrite=True,
#     callback=stow.callbacks.ProgressCallback()
# )

# file_content = stow.get(
#     "k8s://kieran-development-area/exercise-session-processor-55d8c7d8d6-vnt88/home/esp/requirements.txt",
#     callback=stow.callbacks.ProgressCallback()
# )

# print(file_content.decode())

# from kubernetes import client, config, stream
# import tarfile
# import io
# import codecs

# config.load_kube_config(context=None)
# _client = client.CoreV1Api()

# fileBytes = b'content'

# # Create the in memory file buffer - assumed this is posssible as bytes is already in memory
# tarFileBuffer = io.BytesIO()

# # Create the archive file for the transport of the source
# with tarfile.open(fileobj=tarFileBuffer, mode='w:') as tar:
#     tarInfo = tarfile.TarInfo('test-file.txt')
#     tarInfo.size = len(fileBytes)
#     tar.addfile(tarInfo, io.BytesIO(fileBytes))

# # tarFileBuffer.seek(0)
# # # with open('here.txt', 'wb') as handle:
# # #     handle.write(tarFileBuffer.read())

# # bttes = tarFileBuffer.read()

# # # print(str(bttes)[2:-1])

# # print(bttes.decode(encoding='ascii'))

# # exit()

# # # print(binascii.b2a_uu(bttes))

# # # exit()
# # # streamExec = stream.stream(
# # #     _client.connect_get_namespaced_pod_exec,
# # #     'exercise-session-processor-55d8c7d8d6-7fnzq',
# # #     'kieran-development-area',
# # #     command=['/bin/bash'],
# # #     stderr=True,
# # #     stdin=True,
# # #     stdout=True,
# # #     tty=False,
# # #     _preload_content=False
# # # )

# # # stdout, stderr = '', ''
# # # while streamExec.is_open():
# # #     streamExec.update(timeout=5)
# # #     a = streamExec.read_stdout(1)
# # #     if a:
# # #         print(a)
# # #         continue
# # #     b = streamExec.read_stderr(1)
# # #     if b:
# # #         print(b)
# # #         continue

# # #     streamExec.write_stdin(input('command: ').encode('utf-8'))

# with open('temp-file-buffer.tar', 'wb+') as tar_buffer:
#     with tarfile.open(fileobj=tar_buffer, mode='w:') as tar:
#         tar.add('docs')

#     streamExec = stream.stream(
#         _client.connect_get_namespaced_pod_exec,
#         'exercise-session-processor-55d8c7d8d6-k4pdh',
#         'kieran-development-area',
#         command=['tar', 'xvf', '-', '-C', '/home'],
#         stderr=True,
#         stdin=True,
#         stdout=True,
#         tty=False,
#         _preload_content=False
#     )

#     tar_buffer.seek(0)

#     while streamExec.is_open():
#         streamExec.update(timeout=0)
#         if streamExec.peek_stdout():
#             print(f"STDOUT: {streamExec.read_stdout()}")
#         if streamExec.peek_stderr():
#             print(f"STDERR: {streamExec.read_stderr()}")

#         # Read at 4 mb/request
#         print('segment reading')
#         segment = tar_buffer.read(10)
#         if segment:
#             print('written', len(segment))
#             streamExec.write_stdin(segment)

#         # if commands:
#         #     c = commands.pop(0)
#         #     streamExec.write_stdin(c)
#         else:
#             break

#     streamExec.close()

#     exit()









# streamExec = stream.stream(
#     _client.connect_get_namespaced_pod_exec,
#     'exercise-session-processor-55d8c7d8d6-k4pdh',
#     'kieran-development-area',
#     command=['/bin/bash'],
#     stderr=True,
#     stdin=True,
#     stdout=True,
#     tty=False,
#     _preload_content=False
# )



# tarFileBuffer.seek(0)

# # streamExec.write_stdin(f'rm /home/upload-test.tar')

# while streamExec.is_open():
#     # streamExec.update(timeout=1)
#     # if streamExec.peek_stdout():
#     #     print(f"STDOUT: {streamExec.read_stdout()}")
#     # if streamExec.peek_stderr():
#     #     print(f"STDERR: {streamExec.read_stderr()}")

#     # Read at 4 mb/request
#     print('segment reading')
#     segment = tarFileBuffer.read(1024*1024*4)
#     if segment:
#         print('written', len(segment))
#         print(segment)
#         streamExec.write_stdin(f'echo -n -e "{str(bttes)[2:-1]}" >> /home/upload-test.tar\n')

#     # if commands:
#     #     c = commands.pop(0)
#     #     streamExec.write_stdin(c)
#     else:
#         break

# streamExec.write_stdin('tar xf /home/upload-test.tar -C /home\n')
# streamExec.write_stdin('rm /home/upload-test.tar\n')

# streamExec.close()

# exit()

# # while

# # streamExec.write_stdin("\n")



# #  resp = stream(api.connect_post_namespaced_pod_exec, name, 'default',
# #                                              command='/bin/sh',
# #                                              stderr=True, stdin=True,
# #                                              stdout=True, tty=False,
# #                                              _preload_content=False)




# # streamExec.write_stdin("echo test string 1\n")


# # streamExec.write_stdin("echo test string 1\n")
# # line = streamExec.readline_stdout(timeout=5)

# # print(line)
# # streamExec.write_stdin("echo test string 2 >&2\n")
# # line = streamExec.readline_stderr(timeout=5)
# # print(line)

# # streamExec.close()
# # exit()







# # tarFileBuffer.seek(0)

# # print('hello')
# # streamExec.update(timeout=1)
# # if streamExec.peek_stdout():
# #     print(f"STDOUT: {streamExec.read_stdout()}")

# # while True:

# #     segment = input('command: ').encode()
# #     if segment == b'exit':
# #         break
# #     if segment:
# #         streamExec.write_stdin(segment)
# #         print('written', len(segment))

# #     streamExec.update(timeout=1)
# #     if streamExec.peek_stdout():
# #         print(f"STDOUT: {streamExec.read_stdout()}")

# # streamExec.close()


# while streamExec.is_open():
#     streamExec.update(timeout=1)
#     if streamExec.peek_stdout():
#         print(f"STDOUT: {streamExec.read_stdout()}")
#     if streamExec.peek_stderr():
#         print(f"STDERR: {streamExec.read_stderr()}")

#     # Read at 4 mb/request
#     print('segment reading')
#     segment = input('command: ')
#     if segment == b'exit':
#         break
#     if segment:
#         print('written', len(segment))
#         streamExec.write_stdin(segment + '\n')

#     # if commands:
#     #     c = commands.pop(0)
#     #     streamExec.write_stdin(c)
#     else:
#         continue

# streamExec.close()





# stow.put(
#     'docs',
#     'k8s://kieran-development-area/exercise-session-processor-55d8c7d8d6-k4pdh/home/docs',
#     callback=stow.callbacks.ProgressCallback(),
#     overwrite=True
# )