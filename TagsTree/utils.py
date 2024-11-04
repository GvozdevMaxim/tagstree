import string, pymorphy2


class TextClear:
    """Clears the text of unnecessary parts of speech and punctuation marks"""
    functors_pos = {'INTJ', 'PRCL', 'CONJ', 'PREP', 'NPRO'}

    @staticmethod
    def removal_punctuation_marks(text):
        """Removing punctuation."""
        for p in string.punctuation + string.digits + '–' + '—' + '\n':
            if p in text:
                text = text.replace(p, '')
        return text

    @staticmethod
    def get_part_of_speech(word, morth=pymorphy2.MorphAnalyzer()):
        """Identifying part of speech."""
        return morth.parse(word)[0].tag.POS

    def removing_words(self, list_word):
        """"Removing unnecessary parts of speech"""
        return [word for word in list_word if self.get_part_of_speech(word) not in self.functors_pos]


class TagsCounting:
    """Counts the number of tags and sums them with tags from other publications"""
    def __init__(self):
        self.result_dict = {}

    @staticmethod
    def counter(list_element):
        """Count of repetitions of tags in list"""
        count = {}

        for element in list_element:
            if count.get(element, None):
                count[element] += 1
            else:
                count[element] = 1

        return count

    def sum_counter(self, sorted_values):
        """Merging tags from different publications"""
        for k, v in sorted_values.items():
            if self.result_dict.get(k) is None:
                self.result_dict[k] = v
            else:
                self.result_dict[k] += v

        return self.result_dict
