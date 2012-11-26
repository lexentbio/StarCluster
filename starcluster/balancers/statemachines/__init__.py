import datetime

#From http://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons-in-python/33201#33201
class Singleton(type):
    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls.instance = None

    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance

def inheritors(klass):
    subclasses = set()
    work = [klass]
    while work:
        parent = work.pop()
        for child in parent.__subclasses__():
            if child not in subclasses:
                subclasses.add(child)
                work.append(child)
    return subclasses


class StateMachineException(Exception):
    pass

        
class StateMachine(object):

    def __init__(self, loadbalancer, parent_state_class, settings):
        self._lb = loadbalancer;
        self._available_states = inheritors(parent_state_class)
        self._parent_state_class = parent_state_class
        self._latest_state = None
        self._settings = settings
        parent_state_class.validate(self._lb, settings)

    def _getCurrentState(self):
        """
            Will call isInState method on all states of the _available_states list.
            If a single state is found, returns it. Else, raises an Exception.
        """
        self._parent_state_class.preIsInState(self._lb)
        states = []
        for state_class in self._latest_state.valid_transitions:
            state_obj = state_class()
            if state_obj.isInState(self._lb):
                states.append(state_obj)

        if len(states) == 1:
            return states[0]
        if len(states) == 0:
            raise StateMachineException("No state could be associated")
        raise StateMachineException("More than one state could be associated")

    def run(self):
        self._parent_state_class.init(self._lb, self._settings)
        for state_class in self._available_states:
            if state_class.__name__ == "Start":
                self._latest_state = state_class()
                break
        current_state = self._getCurrentState()
        print current_state
        sys.exit()
        while current_state.doTheRightThing(self._lb):
            self._latest_state = current_state 
            current_state = self._getCurrentState()
    
class State(object):
    def __str__(self):
        return self.__class__.__name__

    def isInState(self):
        """
        To implement in the states.
        Returns true if the process is in this state.
        """
        raise Exception("Should be implemented in the state itself")

    def doTheRightThing(self, lb):
        """
        To implement in the states.
        When a state if found, this is called to make it do its business
        """
        raise Exception("Should be implemented in the state itself")



