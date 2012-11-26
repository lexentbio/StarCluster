from starcluster.balancers.statemachines import State
from starcluster.balancers.statemachines import Singleton
from starcluster import static
from starcluster import exception
import datetime
import time

class TfbState(State):

    @staticmethod
    def init(lb, settings):
        class Empty():
            pass
        conn = lb.cluster.ec2.conn
        tfb_state                     = Empty()
        tfb_state.fb_instance_type    = settings["fb_instance_type"]
        tfb_state.fb_image_id         = settings["fb_image_id"]
        tfb_state.fb_spot_bid         = settings["fb_spot_bid"]
        tfb_state.current_sir         = None
        tfb_state.previous_sir        = None
        tfb_state.utc_start_time      = datetime.datetime.utcnow()
        tfb_state.already_tried       = False
        tfb_state.request_type        = None
        tfb_state.sir_creation_thread = None
        tfb_state.stats_expiration    = None
        tfb_state.stats               = None
        try:
            tfb_state.previous_sir = \
                conn.get_all_spot_instance_requests()[-1]
        except:
            #no previous sir
            pass

        lb.tfb_state = tfb_state

    @staticmethod
    def validate(lb, settings):
        #validates fall back img, instance type and spot bid
        lb.cluster.validator.validate_image_settings(settings["fb_image_id"], \
                                           "fb_image_id")
        if not settings["fb_instance_type"] in static.INSTANCE_TYPES:
            raise exception.ClusterValidationError(
                'Incompatible node_image_id and node_instance_type:\n' + e.msg)
        lb.cluster.validator.check_platform(settings["fb_image_id"], 
            settings["fb_instance_type"])

    @staticmethod
    def preIsInState(lb):
        #used to compile required entity prices
        pdesc = "Linux/UNIX"
        conn = lb.cluster.ec2.conn
        prices = {}
        types = [lb.cluster.node_instance_type, lb.tfb_state.fb_instance_type]
        for inst_type in types:
            prices[inst_type] = \
                conn.get_spot_price_history(instance_type=inst_type,
                                      availability_zone="us-east-1a", #TODO: deharcode
                                      product_description=pdesc)[0].price

        lb.tfb_state.decent_hvm_price = prices["cc2.8xlarge"] < \
                                        3 * prices["m2.4xlarge"]

        st = lb.tfb_state.utc_start_time
        wait_for_hvm = False
        limit_1h = \
            datetime.datetime(st.year, st.month, st.day, 1, 0, 0) + \
                datetime.timedelta(days=1)
        lb.tfb_state.now = datetime.datetime.utcnow()

        if not lb.tfb_state.stats_expiration or \
                lb.tfb_state.stats_expiration < datetime.datetime.utcnow():
            lb.tfb_state.stats = lb.get_stats()
            lb.tfb_state.stats_expiration = datetime.datetime.utcnow() + \
                                            datetime.timedelta(minutes=2)

    @staticmethod
    def getSir(lb):
        tfb_state = lb.tfb_state
        conn = lb.cluster.ec2.conn
        while True:
            try:
                sir = conn.get_all_spot_instance_requests()[-1]
                if tfb_state.previous_sir is None or \
                        tfb_state.previous_sir.id != sir.id:
                    tfb_state.current_sir = sir
                    break
            except:
                #no sirs yet
                pass
            time.sleep(5)

class Start(TfbState):
    __metaclass = Singleton

    def __init__(self):
        self.valid_transitions = [CreateHmvInst, CreateMemInst]

    def isInState(self, lb):
        return True

    def doTheRightThing(self, lb):
        return True
        

class CreateHmvInst(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [WaitForInstReq]

    def isInState(self, lb):
        """
        No current sir, not already tried, if past 18h gmt-5, must be bellow
        5 min past the start time, hvm price < 3 * mem price
        """
        tfb_state = lb.tfb_state
        if all([not tfb_state.current_sir, not tfb_state.already_tried, 
                tfb_state.decent_hvm_price]):
            return True
        return False

    def doTheRightThing(self, lb):
        lb.tfb_state.request_type = "hvm"
        kwargs = {"need_to_add" : 1}#TODO
        tfb_state.sir_creation_thread = \
            Process(target=lb.cluster.add_nodes, kwargs=kwargs)
        TfbState.getSir(lb)
        return True

class CreateMemInst(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [WaitForInstReq]

    def isInState(self, lb):
        tfb_state = lb.tfb_state
        if all([not lb.tfb_state.current_sir, 
                tfb_state.already_tried or not tfb_state.decent_hvm_price]):
            return True
        return False

    def doTheRightThing(self, lb):
        lb.tfb_state.request_type = "mem"
        kwargs = {"need_to_add"   : 1,
                  "instance_type" : lb.tfb_state.fb_instance_type,
                  "image_id"      : lb.tfb_state.fb_image_id
                 }
        tfb_state.sir_creation_thread = \
            Process(target=lb.cluster.add_nodes, kwargs=kwargs)
        TfbState.getSir(lb)
        return True

class WaitForInstReq(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [WaitForInstReq, CancelInstReq]

    def isInState(self, lb):
        tfb_state = lb.tfb_state
        if not tfb_state.sir_creation_thread.is_alive():
            return False

        if tfb_state.current_sir and tfb_state.request_type == "mem":
            #when waiting for mem, wait forever
            return True

        st = tfb_state.utc_start_time
        limit_1h = \
            datetime.datetime(st.year, st.month, st.day, 1, 0, 0) + \
                datetime.timedelat(days=1)
        if tfb_state.now < limit_1h_utc or \
                now > st + datetime.timedelta(minutes=6):
            #before 18h (1h utc), wait, otherwise wait at most 6 min
            return True

        return False

    def doTheRightThing(self, lb):
        lb.tfb_state.sir_creation_thread.join(60)
        return True

class CancelInstReq(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [CreateMemInst, Done]

    def isInState(self, lb):
        #the only reason to cancel is a timeout on hvm
        tfb_state = lb.tfb_state
        st = tfb_state.utc_start_time
        if all([tfb_state.current_sir, tfb_state.request_type != "mem",
                tfb_state.now >= limit_1h_utc, 
                tfb_state.now > st + datetime.timedelta(minutes=6)]):
            #when waiting for mem, wait forever
            return True
        return False

    def doTheRightThing(self, lb):
        #cancel sir
        lb.tsb_state.current_sir.cancel()#might still be fulfilled
        lb.tfb_state.sir_creation_thread.join()
        lb.tfb_state.sir_creation_thread = None
        lb.current_sir = None
        return True

class Done(TfbState):
    __metaclass__ = Singleton

    def __init__(self):
        self.valid_transitions = [Done]

    def isInState(self, lb):
        t = lb.tfb_state.sir_creation_thread
        return t and not t.is_alive()

    def doTheRightThing(self, lb):
        return False

