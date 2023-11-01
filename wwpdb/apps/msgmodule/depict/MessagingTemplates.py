##
# File:  MessagingTemplates.py
# Date:  12-Aug-2014
# Updates:
#
#    2014-10-10    RPS    msgTmplt_correspondence abbreviated to eliminate boilerplate that is already contained in correspondence content.
#    2014-10-30    RPS    Removed square brackets surrounding PDB and Deposition IDs
#    2014-11-18    RPS    Now using single template for validation letter and initializing in this class. (i.e. did away with validation-minor vs major issues templates).
#                            New msgTmplt_systemUnlocked template.
#    2015-01-28    RPS    Updated language for approval templates to remove line: "Please acknowledge receipt of this message."
#    2015-03-11    RPS    Improving display of entry title within horiz rules in message templates
#    2015-05-08    RPS    Introducing support for custom message templates relative to different experimental methods.
#    2015-06-12    RPS    "System Unlocked" template updated.
#    2015-09-17    RPS    Migrating template pieces used for email notifications from MessagingIo to this module.
#    2015-10-27    RPS    Introduction of EM specific message templates.
#    2015-10-28    RPS    More updates to support EM specific message templates.
#    2015-12-02    RPS    Removed obsolete validation letter content (validation content no longer generated by Msgmodule)
#    2015-12-10    RPS    Updated manner in which obtaining URL for deposition webpage
#    2016-01-24    RPS    Updated for new template for EM map-only, post-annotation letter
#    2016-03-23    EP     Update urls for policy and procedure links
#    2016-04-21    RPS    Adjusting msgTmplt_systemUnlocked_em to remove clause referring to validation server instructions.
#    2016-05-10    RPS    release message tmplts updated to indicate that release cannot happen sooner than stated dates.
#    2016-08-09    RPS    Changes to support site/annotator specific footers in message templates.
#                            Updates to EM letter templates in order to correctly distinguish between map-only and map+model releases.
#    2016-09-21    RPS    Integrating customized signoff with msgTmplt_vldtn template.
#    2017-08-18    RPS    Accommodating updates in "withdrawn" letter template
#    2017-10-09    RPS    Updating template for "explicit approval". Adjusting signoff content for EM Map Only cases.
#    2018-01-30    RPS    Readjusting signoff content for EM Map Only cases.
#    2019-06-19    EP     Add msgTmplt_remindUnlocked for automatic sending of message for unlocked
#    2022-01-26    CS     update template
#    2022-02-27    CS     add template for Map-only withdrawn
#    2023-10-20    CS     add/modify various templates for superseding entry release, EM model-only and map-only
##
"""
Convenience class to serve as source of message templates for dynamic population

"""
__docformat__ = "restructuredtext en"
__author__ = "Raul Sala"
__email__ = "rsala@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.02"


class MessagingTemplates(object):
    """Convenience class to serve as source of message templates for dynamic population"""

    msgTmplt_closing = """Sincerely yours,

%(full_name_annotator)s

%(annotator_group_signoff)s
%(site_contact_details)s
"""
    msgTmplt_annotatorGroupSignoff = "The wwPDB Biocuration Staff"

    msgTmplt_annotatorGuestPdbjSignoff = "Guest Biocurators of PDBj"

    msgTmplt_site_contact_details_rcsb = """
--------------------------------------------------------------------------------------------------------------
RCSB Protein Data Bank (RCSB PDB), a wwPDB partner site
http://www.rcsb.org
Rutgers, The State University of New Jersey, Piscataway, NJ, USA
Facebook: http://www.facebook.com/RCSBPDB
Twitter: http://twitter.com/#!/buildmodels
--------------------------------------------------------------------------------------------------------------"""

    msgTmplt_site_contact_details_pdbe = """
--------------------------------------------------------------------------------------------------------------
Protein Data Bank in Europe (PDBe), a wwPDB partner site
http://www.PDBe.org

EMBL Outstation Hinxton
European Bioinformatics Institute
Wellcome Trust Genome Campus
Hinxton
Cambridge CB10 1SD UK

http://www.facebook.com/proteindatabank
http://twitter.com/PDBeurope
--------------------------------------------------------------------------------------------------------------"""

    msgTmplt_site_contact_details_pdbj = """
--------------------------------------------------------------------------------------------------------------
Protein Data Bank Japan (PDBj), a wwPDB partner site
http://pdbj.org
Institute for Protein Research, Osaka University, Osaka, Japan
Facebook: https://www.facebook.com/PDBjapan
Twitter: https://twitter.com/PDBj_en
--------------------------------------------------------------------------------------------------------------"""

    msgTmplt_default = """Dear Depositors,

Please acknowledge receipt of this message.


This message concerns your structure %(accession_ids)s (Deposition ID %(identifier)s) entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

Entry authors:

%(entry_authors_newline_list)s


Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_vldtn = """%(starter_msg_body)s

%(msg_closing)s
"""

    msgTmplt_approvalExplicit = """Dear Depositors,

Thank you for your response. This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) has been approved. The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

We have changed the status of the entry to %(status_code)s (%(entry_status)s). Your structure is now on hold until %(expire_date)s. It may be released earlier if the structure is published or you notify us that the structure can be released.

%(doinotice)s
Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_approvalImplicit = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) has been approved. The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

As we have not heard from you since we sent the validation report and processed files to you on %(outbound_rprt_date)s, we have changed the status of the entry to %(status_code)s (%(entry_status)s) with implicit approval in accordance with wwPDB policies, http://www.wwpdb.org/documentation/policy.

Your structure is now on hold until %(expire_date)s. It may be released earlier if the structure is published or you notify us that the structure can be released.

%(doinotice)s
Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_reminder = """Dear Depositors,

Please acknowledge receipt of this message.

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) is still awaiting your input. The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

We have not heard from you since we sent you the validation report, processed PDB and mmCIF files on %(outbound_rprt_date)s. Please respond to the questions raised in our previous correspondence as soon as possible.

In accordance with wwPDB policies (http://www.wwpdb.org/documentation/policy), if we find a publication describing the entry, it will be released with CAVEAT records highlighting any outstanding issues.  If after %(expire_date)s (one year from the deposition date) we are unable to find a publication describing this structure, the entry may be withdrawn if there remain outstanding issues, otherwise the entry will be released.

Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_releaseWthPblctn = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code)s', will be released on %(release_date)s. This is the next available release date.

The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

Your entry has the following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_releaseWthPblctn_supersede = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code)s', will be released on %(release_date)s. This is the next available release date.

On release, this entry will replace %(spr_to_replace_pdb_ids)s in the PDB archive and %(spr_to_replace_pdb_ids)s will be obsoleted.

The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

Your entry has the following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_releaseWthOutPblctn = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code)s', will be released on %(release_date)s.  This is the next available release date.

The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

Your entry has the following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s

"""
    msgTmplt_releaseWthOutPblctn_supersede = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code)s', will be released on %(release_date)s.  This is the next available release date.

On release, this entry will replace %(spr_to_replace_pdb_ids)s in the PDB archive and %(spr_to_replace_pdb_ids)s will be obsoleted.

The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

Your entry has the following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_withdrawn = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code)s', will be withdrawn on %(withdrawn_date)s.

The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

Please do not use these accession IDs in any publications.
%(thurs_wdrn_clause)s

%(msg_closing)s
"""

    msgTmplt_systemUnlocked = """Dear Depositor,

The deposition interface has been unlocked as per your request. If you are providing replacement coordinates, please visit the validation server (http://wwpdb-validation.wwpdb.org/validservice/)
and check your coordinates to ensure the quality of your structure prior to re-submission.  Make sure to press the 'Submit deposition' button once you have finished making your changes within the deposition interface.

%(msg_closing)s
"""

    msgTmplt_systemUnlockedPostRel = """Dear Depositor,

The deposition interface has been unlocked as per your request in order to facilitate replacement of your model. You will be able to upload a replacement model, without any change to experimental data. As this entry has already been released, we will treat this as an entry that is to be released as soon as revisions are complete.
Make sure to press the 'Submit deposition' button once you have finished making your changes within the deposition interface.

%(msg_closing)s
"""

    msgTmplt_remindUnlocked = """Dear Depositors,

Please acknowledge receipt of this message.

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) is still awaiting your input. The entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

This entry was unlocked on %(unlock_date)s per your request. However, the updated files and/or corrections have not been submitted yet. Please upload the new files, make corrections to the data in the deposition interface if needed, and press the 'Submit deposition' button so that we can finalize the processing of your deposition.

Thank you for your attention.

%(msg_closing)s
"""

    ######################################################################
    # TEMPLATE COMPONENTS BELOW ARE USED FOR ELECTRON MICROSCOPY ENTRIES #
    ######################################################################

    msgTmplt_closing_emMapOnly = """Sincerely yours,

%(full_name_annotator)s

%(annotator_group_signoff)s
%(site_contact_details)s
"""

    msgTmplt_annotatorGroupSignoff_em = "The wwPDB Biocuration Staff"

    msgTmplt_annotatorGuestPdbjSignoff_em = "Guest Biocurators of PDBj"

    msgTmplt_default_em = """Dear Depositors,

Please acknowledge receipt of this message.

This message concerns your %(accession_ids)s (Deposition ID %(identifier)s) entitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Entry authors:

%(em_entry_authors_newline_list)s


Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_mapOnly_authStatus_em = """Dear Depositors,

Your %(accession_ids)s which corresponds to Deposition ID %(identifier)s has been annotated and the status is now AUTH.

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Entry authors:

%(em_entry_authors_newline_list)s

Please review the curated data and either reply with your approval or let us know if any additional changes are required. Your approval will be assumed if we do not hear from you within three weeks from the time when the annotation report is made available and assuming there are no major issues with the submission.

When the primary citation associated with your entry is published, please notify us through the deposition system and provide the PubMed ID (if available), journal name, volume, page numbers, title, authors list and DOI.

Please be aware that all public access to your entry will be via the accession code, %(accession_ids)s, which is distinct from the deposition number.

Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_approvalExplicit_em = """Dear Depositors,

Thank you for your response. This message is to inform you that your %(accession_ids)s (Deposition ID %(identifier)s) %(has_have)sbeen approved. The%(entry_entries)s%(is_are)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

We have changed the status of the entry to %(status_code_em_map)s (%(entry_status_em_map)s). Your%(entry_entries)s%(is_are)snow on hold until %(expire_date_em_map)s.%(it_they)smay be released earlier if the structure is published or you notify us that the structure can be released.

%(doinotice)s
Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_approvalImplicit_em = """Dear Depositors,

This message is to inform you that your %(accession_ids)s (Deposition ID %(identifier)s) %(has_have)sbeen approved. The%(entry_entries)s%(is_are)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

As we have not heard from you since we sent the %(vldtn_rprt)sprocessed files to you on %(outbound_rprt_date_em)s, we have changed the status of the entry to %(status_code_em_map)s (%(entry_status_em_map)s) with implicit approval in accordance with wwPDB policies, http://www.wwpdb.org/documentation/policy.html.

Your%(entry_entries)s%(is_are)snow on hold until %(expire_date_em_map)s.%(it_they)smay be released earlier if the structure is published or you notify us that the structure can be released.

%(doinotice)s
Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_reminder_em = """Dear Depositors,

Please acknowledge receipt of this message.

This message is to inform you that your %(accession_ids)s (Deposition ID %(identifier)s) %(is_are)sstill awaiting your input. The entry is entitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

We have not heard from you since we sent you the %(vldtn_rprt)sprocessed files on %(outbound_rprt_date_em)s. Please respond to the questions raised in our previous correspondence as soon as possible.

In accordance with wwPDB policies (http://www.wwpdb.org/documentation/policy.html), if we find a publication describing the %(entry_entries_comma)s%(it_they_lcase)swill be released%(caveat_records)s.
If after %(expire_date_em_map)s (one year from the deposition date) we are unable to find a publication describing%(this_these)s%(entry_entries_comma)s the%(entry_entries)smay be withdrawn if there remain outstanding issues, otherwise the%(entry_entries)swill be released.

Thank you for your attention.

%(msg_closing)s
"""

    msgTmplt_releaseWthPblctn_em = """Dear Depositors,

This message is to inform you that your %(accession_ids_em_rel)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code_em_rel)s', will be released on %(release_date)s.  This is the next available release date.

The%(entry_entries_em_rel)s%(is_are_em_rel)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Your%(entry_entries_em_rel)s%(has_have_em_rel)sthe following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_releaseWthPblctn_em_supersede = """Dear Depositors,

This message is to inform you that your %(accession_ids_em_rel)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code_em_rel)s', will be released on %(release_date)s.  This is the next available release date.

On release, this entry will replace %(spr_to_replace_pdb_ids)s in the PDB archive and %(spr_to_replace_pdb_ids)s will be obsoleted.

The%(entry_entries_em_rel)s%(is_are_em_rel)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Your%(entry_entries_em_rel)s%(has_have_em_rel)sthe following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_releaseWthPblctn_em_map_only = """Dear Depositors,

This message is to inform you that your %(accession_ids_em_rel)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code_map)s', will be released on %(release_date)s.  This is the next available release date.

The%(entry_entries_em_rel)s%(is_are_em_rel)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Your%(entry_entries_em_rel)s%(has_have_em_rel)sthe following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_releaseWthOutPblctn_em = """Dear Depositors,

This message is to inform you that your %(accession_ids_em_rel)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code_em_rel)s', will be released on %(release_date)s.  This is the next available release date.

The%(entry_entries_em_rel)s%(is_are_em_rel)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Your%(entry_entries_em_rel)s%(has_have_em_rel)sthe following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_releaseWthOutPblctn_em_supersede = """Dear Depositors,

This message is to inform you that your %(accession_ids_em_rel)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code_em_rel)s', will be released on %(release_date)s.  This is the next available release date.

On release, this entry will replace %(spr_to_replace_pdb_ids)s in the PDB archive and %(spr_to_replace_pdb_ids)s will be obsoleted.

The%(entry_entries_em_rel)s%(is_are_em_rel)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Your%(entry_entries_em_rel)s%(has_have_em_rel)sthe following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_releaseWthOutPblctn_em_map_only = """Dear Depositors,

This message is to inform you that your %(accession_ids_em_rel)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code_map)s', will be released on %(release_date)s.  This is the next available release date.

The%(entry_entries_em_rel)s%(is_are_em_rel)sentitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Your%(entry_entries_em_rel)s%(has_have_em_rel)sthe following primary citation:

Author(s):   %(citation_authors)s

Title:   %(citation_title)s

Journal:   %(citation_journal_abbrev)s%(citation_journal_volume)s%(citation_pages)s%(citation_year)s%(citation_pubmedid)s%(citation_doi)s

%(doinotice)s%(thurs_prerelease_clause)s

%(msg_closing)s
"""

    msgTmplt_withdrawn_em = """Dear Depositors,

This message is to inform you that your %(accession_ids)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code)s', will be withdrawn on %(withdrawn_date)s.

The entry is entitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Please do not use these accession IDs in any publications.

%(thurs_wdrn_clause)s

%(msg_closing)s
"""

    msgTmplt_withdrawn_em_map_only = """Dear Depositors,

This message is to inform you that your %(accession_ids)s (Deposition ID %(identifier)s) and the associated experimental data which were deposited with release instructions, '%(auth_rel_status_code_map)s', will be withdrawn on %(withdrawn_date)s.

The entry is entitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

Please do not use these accession IDs in any publications.

%(thurs_wdrn_clause_em_map_only)s

%(msg_closing)s
"""

    msgTmplt_systemUnlocked_em = """Dear Depositor,

The deposition interface has been unlocked as per your request. Make sure to press the 'Submit deposition' button once you have finished making your changes within the deposition interface.

%(msg_closing)s
"""

    ####################################################################################################################
    # TEMPLATE COMPONENTS BELOW ARE USED FOR EMAIL NOTIFICATIONS THAT ACCOMPANY MESSAGES FROM ANNOTATORS TO DEPOSITORS #
    ####################################################################################################################

    emailNotif_msgTmplt = """From: <%(sender)s>
To: %(receiver)s
Subject: %(subject)s
%(mime_hdr)s%(msg_content)s
"""

    emailNotif_mimeHdr = "MIME-Version: 1.0\n"
    emailNotif_msgBodyMimeSpec = """Content-Type: text/html; charset="UTF-8"
Content-Transfer-Encoding:8bit
"""
    emailNotif_replyRedirectFooter = """<br /><hr /><br />
In order to reply to the above message you must log into the wwPDB Deposition System at:<br /><br />

   %(tab)s%(dep_email_url)s<br /><br />

"""
    emailNotif_msgBodyMain = (
        """Dear %(email_to_lname)s,<br /><br />

Please be advised that you have a notification message requiring your attention<br />
as relates to your wwPDB deposition with dataset ID: %(dep_id)s%(pdb_block)s
%(entry_title_block)s
%(entry_authors_block)s
The subject of the message is: "%(comm_subject)s"<br /><br />

In order to view the given message %(mention_vldtn_rprts)s in detail, and send any replies, please log into the wwPDB Deposition System at:<br /><br />

   %(tab)s%(dep_email_url)s<br /><br />

and then use the left-hand navigation panel to visit the "Communication" section.<br /><br />

The body of the message is provided below for your convenience:<br /><br /><hr />

%(orig_commui_msg_content)s"""
        + emailNotif_replyRedirectFooter
    )

    emailNotif_msgBodyTmplt = """%(msg_mime_spec)s

%(msg_body_main)s

"""
    #
    emailNotif_pdbBlock = """ and PDB ID: %s<br /><br />

"""
    emailNotif_entryTitleBlock = """Entry Title:<br /><br />

       %(tab)s%(entry_title)s<br /><br />

"""
    emailNotif_entryAuthorsBlock = """Entry Authors:<br /><br />

       %(tab)s%(entry_authors)s<br /><br />

"""

    # ########
    # Obsolete
    # ########
    msgTmplt_obsolete_model = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) will be obsoleted on %(obs_date)s.

The PDB entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

%(obs_special)s%(obs_repl_ids)s%(thurs_obs_clause)s

%(msg_closing)s
"""

    msgTmplt_obsolete_map_only = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) will be obsoleted on %(obs_date)s.

The EMDB entry is entitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

%(obs_special)s%(obs_repl_ids)s%(thurs_obs_clause)s

%(msg_closing)s
"""

    msgTmplt_obsolete_map_model = """Dear Depositors,

This message is to inform you that your structure %(accession_ids)s (Deposition ID %(identifier)s) will be obsoleted on %(obs_date)s.

The PDB entry is entitled:

%(horiz_line)s
%(title)s
%(horiz_line)s

The EMDB entry is entitled:

%(horiz_line)s
%(em_title)s
%(horiz_line)s

%(obs_special)s%(obs_repl_ids)s%(thurs_obs_clause)s

%(msg_closing)s
"""
