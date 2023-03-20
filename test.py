import datetime

print(datetime.datetime.now().strftime("%Y-%m-%d"))


lst = [1, 2, 3, 4, 5]

new = [b + 1 for b in lst]

print(new)
