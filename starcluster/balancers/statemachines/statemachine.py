import traceback
from starcluster.logger import log

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

    def __init__(self, entity, parent_state_class):
        self._entity = entity;
        self._available_states = inheritors(stateClass)
        self._parent_state_class = parent_state_class
        self._latest_state = None

    def _getCurrentState(self):
        """
            Will call isInState method on all states of the _available_states list.
            If a single state is found, returns it. Else, raises an Exception.
        """
        
        self._entity.refresh()
        states = []
        for state_class in self._available_states:
            state_obj = state_class()
            if state_obj.isInState(self._entity):
                states.append(state_obj)

        if len(states) == 1:
            return states[0]
        if len(states) == 0:
            raise StateMachineException("No state could be associated to [%s]" % self._entity.getName())
        raise StateMachineException("More than one state could be associated to [%s]" % self._entity.getName())

    def run(self):
        try:
            current_state = self._getCurrentState()
            while current_state.doTheRightThing(self._entity):
                new_state = self._getCurrentState()
                if not current_state.isValidTransition(new_state):
                    #TODO: error
                current_state = new_state
        except Exception as exc:
            messages.append(exc.message)
            traceback.print_exc(exc)
    
class State(object):
    def __str__(self):
        return self.__class__.__name__

    def isInState(self):
        raise Exception("Should be implemented in the state itself")

    def isValidTransition(self, newState):
        """
            Uses the current_state.validTransitions list to determine
            if the unitary transition toward the newState is valid.
        """
        return newState.__class__ in self.validTransitions

    def doTheRightThing(self, entity):
        """
            Where states do their work. Returns true if the state might have changed
            after the work. Passing state should obviously return false to avoid
            to uselesly recalculate the new state.
        """
        return False;



