from starcluster.statemachine import State
from starcluster.statemachine import Singleton
import datetime

class TfbState(State):

    def init(lb):
        class Empty():
            pass
        conn = lb.cluster.ec2.conn
        tfb_state = Empty()
        tfb_state.current_sir    = None
        tfb_state.previous_sir   = None
        tfb_state.utc_start_time = datetime.datetime.utcnow()
        tfb_state.already_tried  = False
        try:
            tfb_state.previous_sir = 
                conn.get_all_spot_instance_requests()[-1]
        except:
            #no previous sir
            pass

        lb.tfb_state = tfb_state

    def preIsInState(lb):
        #used to compile required entity prices
        pdesc = "Linux/UNIX"
        conn = lb.cluster.ec2.conn
        prices = []
        for inst_type in ["cc2.8xlarge", "m2.4xlarge"]: #TODO: dehardcode
            prices[inst_type] = 
                conn.get_spot_history(inst_type, 
                                      zone="us-east-1a", #TODO: deharcode
                                      product_description=pdesc)

        lb.tfb_state.decent_hvm_price = prices["cc2.8xlarge"] <
                       3 * prices["m2.4xlarge"]


class CreateHmvInst(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [WaitForInstReq]

    def isInState(self, lb):
        """
        No current sir, not already tried, if past 18h gmt-5, must be bellow
        5 min past the start time, hmv price < 3 * mem price
        """
        tfb_state = lb.tfb_state
        if all([not tfb_state.current_sir, not tfb_state.already_tried, 
                tfb.state.decent_price])
            return True
        return False

    def doTheRightThing(self, lb):
        #TODO create HVM request
        pass

class CreateMemInst(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [WaitForInstReq]

    def isInState(self, lb):
        tfb_state = lb.tfb_state
        if all([not lb.tfb_state.current_sir, 
                tfb_state.already_tried or not tfb_state.deent_hmv_price]):
            return True
        return False

    def doTheRightThing(self, lb):
        pass

class WaitForInstReq(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [WaitForInstReq, CancelInstReq]

    def isInState(self, lb):
        if lb.current_sir and #todo price ok for type
            pass
        pass

    def doTheRightThing(self, lb):
        lb.sir_creation_thread.join(60)

class CancelInstReq(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [CreateMemInst, Done]

    def isInState(self, lb):
        pass

    def doTheRightThing(self, lb):
        #cancel sir
        lb.sir_creation_thread.join()
        lb.current_sir = None
        pass

class Done(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [Done]

    def isInState(self, lb):
        pass

    def doTheRightThing(self, lb):
        pass

