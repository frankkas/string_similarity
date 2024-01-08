# string_similarity

## StringExtractor.py
Extracts the strings from a given malware sample. The sample must be unpacked and have the extension "_unpacked" or "_dump7?\_*" according to the Malpedia convention.
The extracted strings are stored in the provided MongoDB. Additionally, they will be saved as a JSON file, so they do not have to be extracted again when the database is refilled.

## StringSimilarityCalculator.py
Calculates the similarity between two malware samples and stores it in MongoDB. For this purpose, the calculated similarities from cosine similarity, decoded similarity, stack similarity, tight similarity, as well as sentence similarity are added in a specific ratio. These must have been calculated beforehand.

## StringSimilarityCalculator_Cosinus.py
Calculates the cosine similarity between two samples and stores it in MongoDB.

## StringSimilarityCalculator_DecodedStrings.py
Calculates the similarity for the decoded strings of two samples and stores it in MongoDB. The similarity is based on the comparison of the sets of decoded strings that are decoded within the same function. 

## StringSimilarityCalculator_StackStrings.py
Calculates the similarity for the stack strings of two samples and stores it in MongoDB. The similarity is based on the comparison of the sets of stack strings that are build within the same function. 

## StringSimilarityCalculator_TightStrings.py
Calculates the similarity for the tight strings of two samples and stores it in MongoDB. The similarity is based on the comparison of the sets of tight strings that are decoded and build within the same function. 

## StringSimilarityCalculator_Sentence.py
Calculates the similarity between the sentences of two samples and stores it in a MongoDB. The similarity is based on the number of similar sentences that contain similar words and have a similar word order.
