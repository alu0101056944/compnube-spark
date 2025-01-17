import re
atlasBoneMeasurementRegExp = r"(?m)((\w+)\s+=\s+\d*\.?\d+)"

text = '''
 DEFINE A greulichPyle ATLAS NAMED miAtlas
  WITH THE FOLLOWING RADIOGRAPHIES
    ONE FOR GENDER male AGE 17.5 WITH
      A Radius BONE OF MEASUREMENTS
        length = 7.6
        width = 16.6

      A Ulna BONE OF MEASUREMENTS
        length = 5.2
        width = 8.7

      Intersesamoids BONE OF MEASUREMENTS
        distance = 1.3

      A Metacarpal BONE OF MEASUREMENTS
        length = 3.7
        widthEpiphysis2 = 8.5
        widthEpiphysis3 = 8.6
        widthEpiphysis4 = 7.2

    ONE FOR GENDER male AGE 18.5 WITH
      A Radius BONE OF MEASUREMENTS
        length = 7.2
        width = 14.4

      A Ulna BONE OF MEASUREMENTS
        length = 4.2
        width = 7.3

      Intersesamoids BONE OF MEASUREMENTS
        distance = 1.0

      A Metacarpal BONE OF MEASUREMENTS
        length = 3.2
        widthEpiphysis2 = 7.2
        widthEpiphysis3 = 7.5
        widthEpiphysis4 = 5.9

    ONE FOR GENDER male AGE 19 WITH
      A Radius BONE OF MEASUREMENTS
        length = 9.5
        width = 20.8

      A Ulna BONE OF MEASUREMENTS
        length = 6.8
        width = 11.0

      Intersesamoids BONE OF MEASUREMENTS
        distance = 0.6

      A Metacarpal BONE OF MEASUREMENTS
        length = 4.4
        widthEpiphysis2 = 9.6
        widthEpiphysis3 = 10.1
        widthEpiphysis4 = 8.4
'''

for match in re.finditer(atlasBoneMeasurementRegExp, text):
  print(match.group())
  print('---')