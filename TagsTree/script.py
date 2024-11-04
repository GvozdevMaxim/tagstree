from database import dbconnection
from utils import TextClear, TagsCounting
from hash import HashingTags


# Select the desired project (int)  interest to you

PROJECT_ID = 2

def tags_tree_builder():
    hashing_tags = HashingTags()
    # 'Selecting publications from the selected project'
    projects = dbconnection.get_projects(PROJECT_ID)

    for proj_id in projects:
        publications = dbconnection.get_publications(*proj_id)

        for content in publications:
            print(content)
            tags_counting_and_sorting, clearing = TagsCounting(), TextClear()
            # 'Removing punctuation'
            text_clear = clearing.removal_punctuation_marks(content[0].lower())
            # 'Text splitting'
            list_words = text_clear.split()
            # "Removing unnecessary parts of speech"
            list_word_without_garbage = clearing.removing_words(list_words)
            # "Count of repetitions of tags in the list"
            tags_with_count = tags_counting_and_sorting.counter(list_word_without_garbage)
            # "Merging tags from different publications"
            non_sorted_lst = tags_counting_and_sorting.sum_counter(tags_with_count)
            # Adding tags to hash
            hashing_tags.insert_hash(*proj_id, non_sorted_lst)

    # 'Unpacking tags from hash for successful insertion into the database'
    tags_to_insert = hashing_tags.unpacking_tags_from_hash_to_insert()

    # Inserting tags in the DB
    for row in tags_to_insert:
        dbconnection.insert_new_tags(row)


if __name__ == "__main__":
    tags_tree_builder()
