class HashingTags:

    def __init__(self):
        self.all_hash = {}

    def insert_hash(self, proj_id, tags):
        if self.all_hash.get(proj_id) is None:
            self.all_hash[proj_id] = dict(tags)
        else:
            for k, v in dict(tags).items():
                if self.all_hash[proj_id].get(k) is None:
                    self.all_hash[proj_id][k] = v
                else:
                    self.all_hash[proj_id][k] += v


    def sorting_tags_in_hash(self, key):
        return sorted(self.all_hash[key].items(), key=lambda tpl: tpl[1], reverse=True)

    def unpacking_tags_from_hash_to_insert(self):
        """Unpacking tags for insertion into the database"""
        unpack_tag_lst = []
        for key in self.all_hash.keys():
            temp = (key,)
            tag_with_values = tuple(self.sorting_tags_in_hash(key))
            for x in range(10):
                temp += tag_with_values[x]
            unpack_tag_lst.append(temp)
        return unpack_tag_lst
