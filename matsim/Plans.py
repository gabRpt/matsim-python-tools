import xopen
import xml.etree.ElementTree as ET
import pandas as pd


class Plans:
    def __init__(self, persons, plans, activities, legs, routes):
        self.persons = persons
        self.plans = plans
        self.activities = activities
        self.legs = legs
        self.routes = routes

def plan_reader(filename, selected_plans_only = False):
    person = None
    tree = ET.iterparse(xopen.xopen(filename), events=['start','end'])
    
    for xml_event, elem in tree:
        if elem.tag == 'person' and xml_event == 'start':
            # keep track of whether a person node has any plans
            this_person_has_plans = False

            if person: person.clear() # clear memory
            person = elem

        elif elem.tag == 'plan' and xml_event == 'end':
            this_person_has_plans = True

            # filter out unselected plans if asked to do so
            if selected_plans_only and elem.attrib['selected'] == 'no': continue

            yield (person, elem)

            # free memory. Otherwise the data is kept in memory
            elem.clear()
        
        elif elem.tag == 'person' and xml_event == 'end':
            # if this person has no plans, then yield the person with a None plan.
            if not this_person_has_plans:
                yield (person, None)

# Parses attributes of an element and adds them to the given dictionary
def _parseAttributes(elem, dict):
    for attrib in elem.attrib:
        dict[attrib] = elem.attrib[attrib]
    return dict

# Returns dataframes with the following relations between them:
# Person : None
# Plan : person_id
# Activity : plan_id
# Leg : plan_id
# Route :leg_id
# The column names of the dataframes are the same as the attribute names (<name:'value'> and <attribute> are parsed)
def plan_reader_dataframe(experienced_plans_filepath, plans_filepath="", selected_plans_only = False):
    experienced_dataframe = _parse_plan_file(experienced_plans_filepath, selected_plans_only=selected_plans_only)
    experienced_activities = experienced_dataframe.activities
    experienced_persons = experienced_dataframe.persons
    experienced_plans = experienced_dataframe.plans
    experienced_legs = experienced_dataframe.legs
    experienced_routes = experienced_dataframe.routes
    
    if plans_filepath != "":
        normal_dataframe = _parse_plan_file(plans_filepath, selected_plans_only=selected_plans_only)
        normal_activities = normal_dataframe.activities
        normal_plans = normal_dataframe.plans
        
        # Create a list of all persons without any activities
        print(experienced_plans)
        print(experienced_activities)
        
        # create a list of persons with no activities
        plans_having_activity = experienced_activities.plan_id.unique()
        persons_without_activity = []
        for plan in experienced_plans.itertuples():
            plan_id = plan.id
            if plan_id not in plans_having_activity:
                persons_without_activity.append(plan.person_id)
            
        # Search all activities of the persons without any activities
        # adding them to experienced_activities
        activities_to_add = []
        for person in persons_without_activity:
            persons_activities = normal_activities[normal_activities['plan_id'] == normal_plans[normal_plans['person_id'] == str(person)]['id'].values[0]]
            activities_to_add += persons_activities.to_dict(orient='records')
        
        experienced_activities = pd.concat([experienced_activities, pd.DataFrame(activities_to_add)])
        
    return Plans(experienced_persons, experienced_plans, experienced_activities, experienced_legs, experienced_routes)



# Parsing the plan file
def _parse_plan_file(filename, selected_plans_only = False):
    plan_tree = ET.iterparse(xopen.xopen(filename), events=['start','end'])
    
    persons = []
    plans = []
    activities = []
    legs = []
    routes = []
    
    current_person = {}
    current_plan = {}
    current_activity = {}
    current_leg = {}
    current_route = {}
    
    # Indicates current parent element while parsing <attribute> element
    is_parsing_person = False
    is_parsing_activity = False
    is_parsing_leg = False
    
    current_person_id = None
    current_plan_id = 0
    current_activity_id = 0
    current_leg_id = 0
    current_route_id = 0
    
    for xml_event, elem in plan_tree:
        if elem.tag in ['person', 'leg', 'activity', 'plan', 'route'] and xml_event == 'end':
            if is_parsing_person:
                persons.append(current_person)
                current_person = {}
                is_parsing_person = False
            
            if is_parsing_activity:
                activities.append(current_activity)
                current_activity = {}
                is_parsing_activity = False
            
            if is_parsing_leg:
                legs.append(current_leg)
                current_leg = {}
                is_parsing_leg = False
            
            if elem.tag == 'plan':
                if elem.attrib['selected'] == 'no' and selected_plans_only: continue
                plans.append(current_plan)
                current_plan = {}
                
            if elem.tag == 'route':
                routes.append(current_route)
                current_route = {}
            
            elem.clear()
        
        # PERSON
        elif elem.tag == 'person':
            current_person['id'] = elem.attrib['id']
            current_person_id = elem.attrib['id']
            is_parsing_person = True
        
        # PLAN
        elif elem.tag == 'plan':
            if elem.attrib['selected'] == 'no' and selected_plans_only: continue
            current_plan_id += 1
            
            current_plan['id'] = current_plan_id
            current_plan['person_id'] = current_person_id
            current_plan = _parseAttributes(elem, current_plan)
        
        # ACTIVITY
        elif elem.tag == 'activity':
            is_parsing_activity = True
            current_activity_id += 1
            
            current_activity['id'] = current_activity_id
            current_activity['plan_id'] = current_plan_id
            current_activity = _parseAttributes(elem, current_activity)
            
        
        # LEG
        elif elem.tag == 'leg':
            is_parsing_leg = True
            current_leg_id += 1
            
            current_leg['id'] = current_leg_id
            current_leg['plan_id'] = current_plan_id
            current_leg = _parseAttributes(elem, current_leg)
        
        
        # ROUTE
        elif elem.tag == 'route':
            current_route_id += 1
            
            current_route['id'] = current_route_id
            current_route['leg_id'] = current_leg_id
            current_route['value'] = elem.text
            current_route = _parseAttributes(elem, current_route)
        
        
        # ATTRIBUTES
        elif elem.tag == 'attribute' and xml_event == 'end':
            attribs = elem.attrib
            
            if is_parsing_activity:
                current_activity[attribs['name']] = elem.text
                
            elif is_parsing_leg:
                current_leg[attribs['name']] = elem.text
            
            elif is_parsing_person: # Parsing person
                current_person[attribs['name']] = elem.text
    
    persons = pd.DataFrame.from_records(persons)
    plans = pd.DataFrame.from_records(plans)
    activities = pd.DataFrame.from_records(activities)
    legs = pd.DataFrame.from_records(legs)
    routes = pd.DataFrame.from_records(routes)
    
    return Plans(persons, plans, activities, legs, routes)