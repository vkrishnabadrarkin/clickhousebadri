import random
def student_info(batch:2016):
    choice_list = ['math','cs','ee','mech','chem','biochem']
    roll_num = random.randint(1,5)
    dept = random.choice(choice_list)
    if dept == 'math':
        batch = batch+1
    return {"id":roll_num,"department":dept,"year":batch}
def student_infos():
    return [student_info(2014) for _ in range(5)]
events = student_infos()
print(events)