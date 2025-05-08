import re
import sys

def matchStr(inAddr, fieldName, inStr):
    matchedPos = None
    goodness = None

    # try striping the head of inStr till match is found
    # to deal with cases like eg. inAddr = 兆康站, inStr = 港鐵兆康站
    for i in range(0, len(inStr)):
        newInStr = inStr[i:]
        matchedPosStart = inAddr.find(newInStr)
        if matchedPosStart >= 0:
            matchedPos = (matchedPosStart, matchedPosStart + len(newInStr))
            goodness = (len(newInStr)/len(inStr) - 0.5)*2
            break

        if (len(inStr) - i) <= 3: break  # give up if remaining inStr too short
        if (i >= len(inStr) // 2): break  # give up if already stripped half

    return [(fieldName, inStr, matchedPos, goodness)]


def matchChiStreetOrVillage(inAddr, inDict):
    """
    inDict is the ChiStreet field of the ogcio result, eg.
    {'StreetName': '彌敦道',
     'BuildingNoFrom': '594'   (may be absent)
     'BuildingNoTo': '596'     (may be absent)
     },

    """
    matches = []            

    key = None
    if 'StreetName' in inDict: key = 'StreetName'
    if 'VillageName' in inDict: key = 'VillageName'

    inStr = inDict[key]
    inStr = inStr.split()[-1]  # to deal with case like '屯門 青麟路'
    streetMatch = matchStr(inAddr, key, inStr)[0]
    matches.append(streetMatch)

    ogcioBNoFrom = inDict.get('BuildingNoFrom', '')
    ogcioBNoTo = inDict.get('BuildingNoTo', '')

    if not ogcioBNoFrom: return matches

    inAddrBNoSpan = None  # the position of the words in the inAddr string
    inAddrBNoFrom = ''
    inAddrBNoTo = ''

    # look for street no. after the street in inAddr
    matchedPos = streetMatch[2]
    if matchedPos != None:
        matchedPosEnd = matchedPos[1]
        inStr = inAddr[matchedPosEnd:]
        reResult = re.match(r'([0-9A-z]+)[至及\-]*([0-9A-z]*)號', inStr)  # this matches '591-593號QWER'
        # print("a", matchedPosEnd, inStr, reResult)
        if reResult:
            inAddrBNoSpan = tuple(matchedPosEnd + x for x in reResult.span())
            inAddrBNoFrom = reResult.groups()[0]
            inAddrBNoTo = reResult.groups()[1]

    if ogcioBNoTo == '': ogcioBNoTo = ogcioBNoFrom
    if inAddrBNoTo == '': inAddrBNoTo = inAddrBNoFrom

    # check overlap between inAddrBNoFrom-To  and ogcioBNoFrom-To
    if (ogcioBNoTo < inAddrBNoFrom or ogcioBNoFrom > inAddrBNoTo):
        inAddrBNoSpan = None  # no overlap so set the matched span to none

    if 'BuildingNoFrom' in inDict:
        goodness = 1. if inAddrBNoFrom==ogcioBNoFrom else 0.5
        matches.append(('BuildingNoFrom', ogcioBNoFrom, inAddrBNoSpan, goodness))
    if 'BuildingNoTo' in inDict:
        goodness = 1. if inAddrBNoTo == ogcioBNoTo else 0.5
        matches.append(('BuildingNoTo', ogcioBNoTo, inAddrBNoSpan, goodness))

    return matches


def matchDict(inAddr, inDict):
    matches = []
    for (k, v) in inDict.items():
        #print (k,v)
        if k == 'ChiStreet':
            matches += matchChiStreetOrVillage(inAddr, v)
        elif k == 'ChiVillage':
            matches += matchChiStreetOrVillage(inAddr, v)
        elif type(v) == dict:
            matches += matchDict(inAddr, v)
        elif type(v) == str:
            matches += matchStr(inAddr, k, v)
        # Not printing any error here
        # else:
        #     print("Unhandled content: ", k, v)
    return matches


class Similarity:
    score = 0
    inAddr = ''
    inAddrHasMatch = []
    ogcioMatches = {}

    def __repr__(self):

        outStr = ''
        outStr += "query: %s\n" % self.inAddr

        tmp = "".join([ s if self.inAddrHasMatch[i] else '?' for (i,s) in enumerate(self.inAddr)])
        outStr += "match: %s\n" % tmp

        outStr += "ogcioMatches: %s\n"% self.ogcioMatches

        outStr += "Score: %s\n" % self.score

        return outStr

def getSimilarityWithOGCIO(inAddr, ogcioResult):
    """
    :param inAddr: a string of address
    :param ogcioResult: the "ChiPremisesAddress" of OGCIO query returned json
    :return:
    """

    matches = matchDict(inAddr, ogcioResult)
    #print (matches)

    inAddrHasMatch  = [False for i in range(len(inAddr))]
    score = 0

    scoreDict = {
        'Region' : 10,
        'StreetName' : 20,
        'VillageName': 20,
        'EstateName' : 20,
        'BuildingNoFrom': 30,
        'BuildingNoTo' :30,
        'BuildingName' : 40,
    }

    for (fieldName, fieldVal, matchSpan, goodness) in matches:
        if matchSpan==None:
            score-=1
            continue

        # if fieldName not in scoreDict:
        #     print("ignored ", fieldName, fieldVal)
        #     print(ogcioResult)

        score += scoreDict.get(fieldName,0) * goodness
        for i in range(matchSpan[0],matchSpan[1]) : inAddrHasMatch[i] = True


    s = Similarity()
    s.score = score
    s.inAddr = inAddr
    s.inAddrHasMatch = inAddrHasMatch
    s.ogcioMatches = matches

    return s

def ParseAddress(result, input_address):
    """
    Parses and ranks address results based on similarity to the input address.

    This function takes a list of address results and an input address, calculates
    the similarity between each result and the input address, sorts the results
    based on the similarity score, and returns the best match.

    Args:
        result (list): A list of dictionaries containing address information.
            Each dictionary should have a 'chi' key with the Chinese address.
        input_address (str): The original input address to compare against.

    Returns:
        dict: The best matching address result, which is the first item in the
            sorted list of results.

    Raises:
        Exception: If any error occurs during the parsing process. The exception
            details, including the line number and error message, are printed.

    Note:
        - The function modifies the input 'result' list by adding a 'match' key
          to each dictionary, containing the similarity score.
        - The similarity is calculated using the getSimilarityWithOGCIO function
          from the util module.
        - Results are sorted in descending order based on the similarity score.
    """
    try:
        # print(result)
        for (idx, aResult) in enumerate(result):
            result[idx]['match'] = getSimilarityWithOGCIO(input_address, aResult['chi'])
        result.sort(key=lambda x: x['match'].score, reverse=True)
        return result[0]
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f'util.py:ParseAddress():line_num={exc_tb.tb_lineno},exception={e}')


def flattenOGCIO(response_json):
    """
       Flattens the OGCIO API address response JSON.

       This function takes a JSON response from the OGCIO address parsing service and flattens it
       into a more manageable list of dictionaries. Each dictionary in the output list represents
       a single address with key information extracted from the original nested structure.

       Args:
           response_json (list): A list of dictionaries containing the OGCIO address response data.

       Returns:
           flat_result (list): A list of flattened dictionaries, each containing:
               - rank (int): The index of the address in the original response.
               - chi (str): Chinese premises address.
               - eng (str): English premises address.
               - geo (dict): Geospatial information of the address.
               - OGCIO_score (float): Validation score provided by OGCIO.

       Raises:
           Exception: If there's an error during the flattening process, it prints an error message
                      with the line number and exception details.

       """
    try:
        flat_result = []
        for idx, addr in enumerate(response_json):
            temp = {
                'rank': idx,
                'chi': addr['Address']['PremisesAddress']['ChiPremisesAddress'],
                'eng': addr['Address']['PremisesAddress']['EngPremisesAddress'],
                'geo': addr['Address']['PremisesAddress']['GeospatialInformation'],
                'OGCIO_score': addr['ValidationInformation']['Score'],
            }
            flat_result.append(temp)
        return (flat_result)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        flattenOGCIO_error = f'util.py:flattenOGCIO():line_num={exc_tb.tb_lineno},exception={e}'
        print(flattenOGCIO_error)

def removeFloor(inputAddress):
 return re.sub(r"([0-9A-z\s\-]+[樓層]|[0-9A-z號\s\-]+[舖鋪]|地[下庫]|平台).*", "", inputAddress)
