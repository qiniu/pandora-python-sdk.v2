class Record(dict):
    """
    Enable dot access to members of a dictionary.
    """
    sep = '.'

    def __call__(self, *args):
        if len(args) == 0: 
            return self
        return Record((key, self[key]) for key in args)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError: 
            raise AttributeError(name)

    def __delattr__(self, name):
        del self[name]

    def __setattr__(self, name, value):
        self[name] = value

    @staticmethod
    def fromkv(k, v):
        result = record()
        result[k] = v
        return result

    def __getitem__(self, key):
        if key in self:
            return dict.__getitem__(self, key)
        key += self.sep
        result = record()
        for k,v in iter(self.items()):
            if not k.startswith(key):
                continue
            suffix = k[len(key):]
            if '.' in suffix:
                ks = suffix.split(self.sep)
                z = result
                for x in ks[:-1]:
                    if x not in z:
                        z[x] = record()
                    z = z[x]
                z[ks[-1]] = v
            else:
                result[suffix] = v
        if len(result) == 0:
            raise KeyError("No key or prefix: %s" % key)
        return result
    

def record(value=None): 
    """
    Return a :class:`Record` instance with value provided.
    :param `value`: An initial record value. type ``dict``
    """
    if value is None: 
        value = {}
    return Record(value)