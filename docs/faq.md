# Frequently asked questions

## Can stow objects be seralised?

**Yes**, `Managers` and `Artefacts` can be seralised using `pickle`. When they are deseralised they will correctly unpack such that there aren't multiple definitions.

```python
>>> import stow
>>> import pickle
>>>
>>> s3 = stow.connect(manager='s3', bucket='example-bucket')
>>> file = s3['/file1.txt']
>>>
>>> ds3 = pickle.loads(pickle.dumps(s3))
>>> s3 == ds3
True
>>> ds3['/file1.txt'] == file
True
>>> file == pickle.loads(pickle.dumps(file))
True
>>> id(file) == id(pickle.loads(pickle.dumps(file)))
```

What this allows you to do is seralise artefacts and pass them to sub-processes to perform work. **This is something that the base packages (boto3 as an example) cannot do**

```python
import stow
import multiprocessing

pool = multiprocessing.Pool(4)

def mpManagerLSFunc(manager):
    return {x.name for x in manager.ls()}

result = pool.map(mpManagerLSFunc, [self.manager]*8)

assertEqual([{x.name for x in self.manager.ls()}]*8, result)
```

```python
import stow
import multiprocessing

pool = multiprocessing.Pool(4)

result = pool.map(lambda x: x.content, manager.ls())

assertEqual(set(x.content for x in manager.ls()), set(result))
```

