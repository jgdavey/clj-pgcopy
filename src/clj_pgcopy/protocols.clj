(ns clj-pgcopy.protocols)

(defprotocol IPGBinaryWrite
  (write-to [this ^DataOutputStream out]))
