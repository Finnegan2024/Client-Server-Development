#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 24 15:37:49 2025

@author: finneganthoma_snhu
"""

from pymongo import MongoClient
from bson.objectid import ObjectId


""" creating helper funcitons to create modular and reusable code within the 
CRUD methods """
def is_dict(data:any) -> bool: # checking if data is a dict
    return True if type(data) == dict else False


def is_not_none(data:any) -> bool: # checking if data is not None
    return True if data is not None else False


def is_not_empty(data:any) -> bool: # verifying data is not empty
    return True if len(data) > 0 else False


# Verifying that the input data is acceptable for corresponding method
# Some methods are allowed empty dicts which can be dictated by second param
def validate_dict(data:any, require_non_empty=True) -> tuple[bool, str]:
    if not is_not_none(data):
        return False, 'Invalid: Param is None'
    if not is_dict(data):
        return False, 'Invalid: Param is not a dictionary'
    if not is_not_empty(data) and require_non_empty:
        return False, 'Invalid: Param is empty'
    return True, 'Param is valid'


# ensuring the '_id' attribute is transformed from a string to an ObjectId type
def validate_dict_id(data:dict) -> dict:
    if '_id' in data and type(data['_id']) == str:
        data['_id'] = ObjectId(data['_id'])
    return data


class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """
    
    
    def __init__(self, user, passwrd):
        """ Initializing the MongoClient. This helps to access the MongoDB
        databases and collections. This is hard-wired to use the aac database,
        the animals collection, and the aac user """
        
        # Connection variables
        USER = user
        PASS = passwrd
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 31735
        DB = 'AAC'
        COL = 'animals'
        
        # Initialize connection
        self.client = MongoClient('mongodb://%s:%s@%s:%d' %
                                  (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]
        
    
    def create(self, data:dict) -> bool:
        """ Method to insert a document into a specified MongoDB database
        and collection """
        
        # calling helper function and returning error if false
        is_valid, reason = validate_dict(data) 
        if not is_valid:
            raise ValueError(reason)
        
        # transforming id type from string to objectid type
        data = validate_dict_id(data)
        
        # invoking database function to insert a new animal
        result = self.collection.insert_one(data)
        # returning success status as True or False
        return result.acknowledged
            
            
    def read(self, data:dict) -> list:
        """ Method to query documents from specified MongoDB database
        and collection """
        
        # calling helper function and returning error if false
        is_valid, reason = validate_dict(data, require_non_empty=False) 
        if not is_valid:
            raise ValueError(reason)
        
        # transforming id type from string to objectid type
        data = validate_dict_id(data)
        
        """ invoking database function to find doc/s, if empty dict then
        returning all docs """
        result = list(self.collection.find(data))
        # if a non empty list then returning list, else returning empty list
        if len(result) != 0:
            return result
        else:
            return list()
        
    def update(self, filter_data:dict, updated_data:dict) -> int:
        """ Method to update document/s from specified MongoDB database
        and collection"""
        
        # calling helper function and returning error if false for filter doc
        is_valid, reason = validate_dict(filter_data,
                                         require_non_empty=True) 
        if not is_valid:
            raise ValueError(reason)
        
        # transforming id type from string to objectid type for filter doc
        filter_data = validate_dict_id(filter_data)
        
                
        # calling helper function and returning error if false for doc update
        is_valid2, reason2 = validate_dict(updated_data, 
                                           require_non_empty=True) 
        if not is_valid2:
            raise ValueError(reason2)
        
        # transforming id type from string to objectid type for doc update
        updated_data = validate_dict_id(updated_data)
        
        # update the specified doc with the specified update
        result = self.collection.update_many(filter_data, 
                                             {'$set': updated_data})
        # returning count of modified docs
        return result.modified_count
            
            
    def delete(self, data:dict) -> int:
        """ Method to delete a document/s from a specified MongoDB 
        database and collection """
                
        # calling helper function and returning error if false
        # requiring the doc to be non-empty to prevent deleting all docs
        is_valid, reason = validate_dict(data, require_non_empty=True) 
        if not is_valid:
            raise ValueError(reason)
        
        # transforming id type from string to objectid type
        data = validate_dict_id(data)
        
        # deleting specified doc/s, if empty dic then deleting all docs
        result = self.collection.delete_many(data)
        return result.deleted_count