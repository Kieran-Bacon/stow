# Put

We are in a situation where we would like to put **x** at **y**

Assumption one: **x** must exist - it is either a file or a directory

Assumption two: **y** is not gurarenteed to exist - the directory that it would be might also not exist

## logic 1

regardless of what X is, it will be put at the location y


if y does not exist:
    put x at y

this would lead to behaviour
    - put("file1.txt", "/home/ubuntu/file1.txt") - puts the file at file1.txt
    - put("file1.txt", "/home/ubuntu/file) - to assume that file is the new name and it becomes a file
    - put("", "/home/ubuntu/directory/file) - this will create the directory and then it will create the file file

so to put 5 things into a directory:

for file in localDirectory:
    put(file, "/home/ubuntu/directory") would end up breaking as it would be putting each file at the same location
    put(file, "/home/ubuntu/directory/") would break as it is a invalid path

requires

stow.mkdir(directory, ignore_exist=True)
for file in localDirectory:
    put(file, directory) would end up breaking as it would be putting each file at the same location
    put(file, directory) would break as it is a invalid path



### If Y exists

currently, if Y exists then it is removed and it is replaced

if Y is a directory:
    if X is a file:
        put x in directory

    if X is a directory:
        put X in Y

